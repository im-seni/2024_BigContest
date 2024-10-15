import os
import time
import yaml
import json
import requests
import argparse
import pointpats
import numpy as np
import pandas as pd
import urllib.parse
import urllib.request

from tqdm import tqdm
from datetime import datetime
from pyproj import Transformer
from openrouteservice import convert
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from ratelimit import limits, sleep_and_retry

ONE_MINUTE = 60
MAX_CALLS_PER_MINUTE = 40

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True, help='choose config file')
    return parser.parse_args()

def validate_config(config):
    
    def validate_mode(config):
        coordinates_only = config.get('coordinates_only', None)
        fixed_origins_only = config.get('fixed_origins_only', None)
        if coordinates_only is None or fixed_origins_only is None:
            print("오류: 'coordinates_only' 또는 'fixed_origins_only'가 누락되었습니다.")
            return
        if coordinates_only and fixed_origins_only:
            print("오류: 'coordinates_only'와 'fixed_origins_only'는 동시에 True일 수 없습니다.")
            return
        else:
            if coordinates_only==True and fixed_origins_only==False:
                print("모드 설정이 올바릅니다: [C]")
            elif coordinates_only==False and fixed_origins_only==False:
                print("모드 설정이 올바릅니다: [R]")
            else:
                print("모드 설정이 올바릅니다: [F]")
    
    def check_fields(required_fields):
        for field, field_type in required_fields.items():
            if field not in config or not isinstance(config[field], field_type):
                print(f"오류: '{field}'가 누락되었거나 {field_type.__name__} 타입이 아닙니다.")
                return
            else:
                print(f"'{field}'가 올바르게 설정되었습니다.")

    def validate_required_fields(config):
        required_fields = {
            'SGIS_consumer_key': str,
            'SGIS_consumer_secret': str,
            'ors_token': str,
            'od_data_path': str,
            'stay_data_path': str,
            'dong_code_path': str,
            'save_directory': str,
            'error_directory': str
        }
        check_fields(required_fields)
    
    def vaidate_mode_C(config):
        required_fields = {
            'date': str,
            'destination': str,
            'view_all_day': bool,
            'time': str,
            'probability': float,
        }
        check_fields(required_fields)

    def vaidate_mode_R(config):
        required_fields = {
            'date': str,
            'destination': str,
            'view_all_day': bool,
            'time': str,
            'probability': float,
            'coordinates_path': str
        }
        check_fields(required_fields)

    def vaidate_mode_F(config):
        required_fields = {
            'fixed_origins': list,
            'fixed_destination': list,
            'profile': str
        }
        check_fields(required_fields)

    validate_mode(config)
    validate_required_fields(config)
    if config['coordinates_only']==True and config['fixed_origins']==False:
        vaidate_mode_C(config)
    elif config['coordinates_only']==False and config['fixed_origins']==False:
        vaidate_mode_R(config)
    else:
        vaidate_mode_F(config)
    print("검증이 완료되었습니다.")


@sleep_and_retry
@limits(calls=MAX_CALLS_PER_MINUTE, period=ONE_MINUTE)
def call_api(url, body, headers):
    """
    openrouteservice API 호출할때 1분당 쿼리 40개 제한을 잘 지키자!
    """
    response = requests.post(url, json=body, headers=headers)
    response.raise_for_status()
    return response.json()


def convert_time(time: str) -> datetime.time:
    """
    원본 csv에 MM:DD 형식의 문자열로 저장되어 있는 시간 정보를 datetime.time 객체로 반환.
    """
    return datetime.strptime(time, '%H:%M').time()


def convert_coord(coord: list) -> list:
    """
    EPSG:5179 좌표계를 OSM과 동일한 EPSG:4326 좌표계로 반환.
    """
    x, y = coord
    transformer = Transformer.from_crs("EPSG:5179", "EPSG:4326", always_xy=True)
    longitude, latitude = transformer.transform(x, y)
    return [longitude, latitude]


def load_origins(config: dict) -> pd.DataFrame:
    od_ct = [1]
    or_cd = config['fixed_origins']
    dt_cd = [config['fixed_destination']] * len(config['fixed_origins'])
    md = [config['profile']] * len(config['fixed_origins'])
    ct = [1] * len(config['fixed_origins'])
    od_data = pd.DataFrame(list(zip(or_cd, dt_cd, md, ct)),
                           columns=['origin_coordinates','destination_coordinates','modal','od_cnts'])
    return od_data


def load_data(config: dict) -> pd.DataFrame:
    """
    사용자가 원하는 날짜와 시간대에 맞춘 csv 정보 불러오기.
    """
    date = config['date']
    time = config['time']
    view_all_day = config['view_all_day']
    od_file = os.path.join(config['od_data_path'], 'od_2023'+date+'_1.csv')
    od_data = pd.read_csv(od_file)
    od_data['start_time'] = od_data['start_time'].apply(convert_time)
    od_data['end_time'] = od_data['end_time'].apply(convert_time)
    if view_all_day:
        print(f'{date[:2]}월 {date[2:]}일 데이터를 전부 불러옵니다.')
    else:
        print(f'{date[:2]}월 {date[2:]}일 {time}시 데이터를 불러옵니다.')
        time = convert_time(time)
        od_data = od_data[(od_data['start_time'] <= time) & (od_data['end_time'] >= time)]
    return od_data


def get_access_token(config: dict) -> str:
    """
    SGIS API 토큰 받아오기.
    """
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
    params = {
        "consumer_key": config['SGIS_consumer_key'],
        "consumer_secret": config['SGIS_consumer_secret']
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    request = urllib.request.Request(full_url)
    with urllib.request.urlopen(request) as response:
        response = json.loads(response.read().decode('utf-8'))
        accessToken = response['result']['accessToken']
    return accessToken


def get_destination(config: dict, accessToken: str) -> tuple:
    """
    사용자 설정 목적지의 도로명 주소를 SGIS API에 통과시켜 좌표와 행정동 코드를 받아오기.
    """
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/geocodewgs84.json"
    params = {
        "accessToken": accessToken,
        "address": config['destination']
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    try:
        request = urllib.request.Request(full_url)
        with urllib.request.urlopen(request) as response:
            response = json.loads(response.read().decode('utf-8'))
            response = response['result']['resultdata'][0]
            dest_code_api = int(response['adm_cd'])
            dest_long = float(response['x'])
            dest_lati = float(response['y'])
    except Exception:
        print(f"목적지에 대한 정보를 불러오는데 실패했습니다: {config['destination']}")
    return dest_code_api, dest_long, dest_lati


def filter_data(config: dict) -> pd.DataFrame:
    """
    사용자 목적지가 속한 행정동을 목적지로 삼는 케이스만 필터링.
    """
    od_data = load_data(config)
    accessToken = get_access_token(config)
    dest_code_api, dest_long, dest_lati = get_destination(config, accessToken)
    code_df = pd.read_json(config['dong_code_path'])
    try:
        code_df = code_df[code_df['API_행정동코드'] == dest_code_api]
        if len(code_df)==0: raise Exception
        dest_code_json = code_df['행정동코드'].iloc[0]
    except Exception:
        print(f"목적지 행정동코드 대조에 실패했습니다: {dest_code_api}")
    od_data = od_data[od_data['dest_hdong_cd']==dest_code_json]
    od_data.reset_index(inplace=True, drop=True)
    return dest_code_api, dest_long, dest_lati, od_data


def check_polygon(coordinates: np.array) -> Polygon:
    """
    행정경계구역을 표시하는 좌표 데이터를 차원 수에 따라 Polygon/MultiPolygon 객체로 지정해주기.
    """
    if coordinates.ndim == 2:
        polygon = Polygon(coordinates)
        return polygon
    elif coordinates.ndim == 3:
        polygons = [Polygon(p) for p in coordinates]
        polygon = MultiPolygon(polygons)
        return polygon
    

def create_origin_coordinates(config: dict, od_data: pd.DataFrame) -> pd.DataFrame:
    """
    출발지 행정경계구역을 불러와 랜덤하게 좌표 샘플링.
    """
    code_df = pd.read_json(config['dong_code_path'])
    origin_coordinates = []
    for code in od_data['origin_hdong_cd']:
        coordinates = code_df.loc[code_df['행정동코드'] == code, '행정경계구역'].values[0]
        coordinates = np.array(coordinates)
        polygon = check_polygon(coordinates)
        random_point = pointpats.random.poisson(polygon, size=1)
        random_point = convert_coord(random_point)
        origin_coordinates.append(random_point)
    od_data['origin_coordinates'] = origin_coordinates
    return od_data


def create_destination_coordinates(config: dict,
                                   od_data: pd.DataFrame,
                                   dest_long: float,
                                   dest_lati: float,
                                   dest_code_api: int) -> pd.DataFrame:
    """
    도착지 행정경계구역으로부터 랜덤하게 샘플링한 좌표들과 목적지 좌표를 축제참여확률에 맞춰 추가.
    """
    probability = config['probability']
    total = len(od_data)
    visit_y = int(total*probability)
    visit_n = total - visit_y
    code_df = pd.read_json(config['dong_code_path'])
    coordinates = code_df.loc[code_df['API_행정동코드'] == dest_code_api, '행정경계구역'].values[0]
    coordinates = np.array(coordinates)
    polygon = check_polygon(coordinates)
    random_points = pointpats.random.poisson(polygon, size=visit_n)
    random_points = list(map(convert_coord, random_points))
    fixed_points = [dest_long, dest_lati]
    fixed_points = np.tile(fixed_points, [visit_y,1])
    if len(random_points) == 0:
        destination_coordinates = fixed_points
    elif len(fixed_points) == 0:
        destination_coordinates = random_points
    else:
        destination_coordinates = np.vstack([random_points, fixed_points])
    np.random.shuffle(destination_coordinates)
    destination_coordinates = destination_coordinates.tolist()
    od_data['destination_coordinates'] = destination_coordinates
    return od_data


def create_paths(config, od_data):
    print(f"현재 데이터는 {len(od_data)}개 입니다.")
    print(f"openrouteservice의 일일 쿼리 제한은 2000 입니다.")
    while True:
        num_query = int(input("원하는 검색 횟수를 입력해주세요: "))
        if num_query > 2000:
            print("입력된 값이 너무 큽니다. openrouteservice의 제한은 2000입니다.")
        elif num_query > len(od_data):
            print(f"입력된 값이 너무 큽니다. 주어진 데이터는 {len(od_data)}개 입니다.")
        elif num_query < 1:
            print("검색 횟수는 1 이상이어야 합니다.")
        else:
            od_data = od_data.sample(n=num_query)
            break

    modal_dict = {
        0.0: 'driving-car',
        1.0: 'driving-hgv',
        2.0: 'driving-car',
        3.0: 'foot-walking',
        4.0: 'cycling-regular',
        5.0: 'driving-car',
        6.0: 'driving-hgv',
        7.0: 'driving-car',
    }

    routes = []
    for _, data in tqdm(od_data.iterrows(), total=len(od_data)):
        coords = [data['origin_coordinates'],data['destination_coordinates']]
        try:
            body = {"coordinates":coords,
                    "preference":"recommended",
                    "radiuses":[2000, 2000]}
            headers = {'Accept': 'application/json, application/geo+json, application/gpx+xml, img/png; charset=utf-8',
                       'Authorization': config['ors_token'],
                       'Content-Type': 'application/json; charset=utf-8'}
            if config['fixed_origins_only']:
                url = os.path.join('https://api.openrouteservice.org/v2/directions', config['profile'])
            else:
                url = os.path.join('https://api.openrouteservice.org/v2/directions', modal_dict[data['modal']])
            response = call_api(url, body, headers)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("1분간 요청 가능한 쿼리의 수를 초과했습니다.")
                print("10초간 대기 후 쿼리를 다시 보냅니다.")
                time.sleep(10)
                response = call_api(url, body, headers)
            else:
                print(f"HTTP Error: {e}")
                route = "Error: " + str(e)

        try:
            route = response["routes"][0]['geometry']
            route = convert.decode_polyline(route)
        except KeyError as e:
            print(f"API 응답에 루트 정보가 없습니다: {e}")
            route = "Error: " + str(e)
        finally:
            routes.append(route)

    od_data['route'] = routes
    return od_data


def collect_errors(config, od_data):
    error_directory = config['error_directory']
    if not os.path.exists(error_directory):
        os.makedirs(error_directory)
    errors = od_data[od_data['route'].apply(lambda x: isinstance(x, str))]
    if not errors.empty:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_path = os.path.join(error_directory, f"errors_{timestamp}.csv")
        errors.to_csv(error_path, index=False)
        od_data = od_data[~od_data['route'].apply(lambda x: isinstance(x, str))].reset_index(drop=True)
    return od_data


def save(config, od_data):
    coordinates_only = config['coordinates_only']
    fixed_origins_only = config['fixed_origins_only']
    save_dir = config['save_directory']
    name = config['destination'].replace(" ", "")
    date = config['date']
    prob = config['probability']
    num = len(od_data)
    if config['view_all_day']==False:
        time = config['time'][:2] + config['time'][3:]
    else:
        time = 'ALL'
    middle_dir = str(name)+'_D'+str(date)+'_T'+str(time)+'_P'+str(prob)

    if coordinates_only:
        save_dir = os.path.join(save_dir, middle_dir, 'coordinates')
        file_name = 'coordinates.json'
        columns=['origin_hdong_cd','dest_hdong_cd','origin_coordinates',
                 'destination_coordinates','start_time','end_time','gender','age','modal',
                 'origin_purpose','dest_purpose','od_dist_avg','od_duration_avg','od_cnts']
    elif fixed_origins_only:
        save_dir = os.path.join(save_dir, 'fixed_origins')
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f'{current_time}.json'
        columns=['origin_coordinates','destination_coordinates','modal','route', 'od_cnts']
    else:
        save_dir = os.path.join(save_dir, middle_dir, 'routes')
        file_name = f'routes_{num}_samples.json'
        columns=['origin_hdong_cd','dest_hdong_cd','origin_coordinates',
                 'destination_coordinates','start_time','end_time','gender','age','modal',
                 'origin_purpose','dest_purpose','od_dist_avg','od_duration_avg','od_cnts','route']
        
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    file_path = os.path.join(save_dir,file_name)
    config_path = os.path.join(save_dir,'last_checkpoint.yaml')
    od_data = od_data[columns]

    with open(config_path, 'w', encoding='utf-8') as file:
        yaml.dump(config, file, allow_unicode=True)

    with open(file_path, 'w', encoding='utf-8') as f:
        od_data.to_json(f, orient='records', force_ascii=False)


if __name__ == "__main__":
    args = parse_args()

    with open(args.config, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    print('============================================')
    print('사용자 설정 검증 중...')
    validate_config(config)

    if config['coordinates_only']:
        print('============================================')
        print('데이터 불러오는 중...')
        dest_code_api, dest_long, dest_lati, od_data = filter_data(config)
        print('============================================')
        print('출발지 좌표 세팅 중...')
        od_data = create_origin_coordinates(config, od_data)
        print('============================================')
        print('도착지 좌표 세팅 중...')
        od_data = create_destination_coordinates(config, od_data, dest_long, dest_lati, dest_code_api)
        print('============================================')
        print('결과 저장 중...')
        save(config, od_data)
    elif config['fixed_origins_only']:
        print('============================================')
        print('죄표 불러오는 중...')
        od_data = load_origins(config)
        print('============================================')
        print('경로 생성 중...')
        od_data = create_paths(config, od_data)
        print('============================================')
        print('에러 케이스 수집 중...')
        od_data = collect_errors(config, od_data)
        print('============================================')
        print('결과 저장 중...')
        save(config, od_data)
    else:
        print('============================================')
        print('좌표 정보 불러오는 중...')
        od_data = pd.read_json(config['coordinates_path'])
        print('============================================')
        print('경로 생성 중...')
        od_data = create_paths(config, od_data)
        print('============================================')
        print('에러 케이스 수집 중...')
        od_data = collect_errors(config, od_data)
        print('============================================')
        print('결과 저장 중...')
        save(config, od_data)