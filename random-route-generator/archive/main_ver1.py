import os
import re
import time
import yaml
import json
import argparse
import pointpats
import numpy as np
import pandas as pd
import urllib.parse
import urllib.request
import openrouteservice

from tqdm import tqdm
from datetime import datetime
from pyproj import Transformer
from shapely.geometry import Polygon
from openrouteservice import convert

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', type=str, required=True, help='choose config file')
    return parser.parse_args()


def convert_to_time(time_string):
    """
    - Arguments
        - 원본 csv에 문자열로 저장되어 있는 시간 정보.
    - Purpose
        - time 객체로 변환.
    - Returns
        - time 객체 반환.
    """
    return datetime.strptime(time_string, '%H:%M').time()


def convert_coordinates(coords):
    """
    - Arguments
        - EPSG:5179 좌표계
    - Purpose
        - OSM과 동일한 EPSG:4326 좌표계로 변환
    - Returns
        - EPSG:4326 좌표계
    """
    x_coords = coords[:, 0]
    y_coords = coords[:, 1]
    transformer = Transformer.from_crs("EPSG:5179", "EPSG:4326", always_xy=True)
    longitude, latitude = transformer.transform(x_coords, y_coords)
    return np.array(list(zip(latitude, longitude)))


def format_dong_name(name):
    """
    - Arguments
        - 원본 csv로부터 가져온 읍면동명 데이터.
    - Purpose
        - SGI 지오코딩 API에 올바른 입력값으로 넘겨주기 위한 문자열 전처리.
    - Returns
        - 문자열 반환.
    """
    pattern1 = r'([ㄱ-ㅣ가-힣]+)(\d가)(제)(\d동)'
    pattern2 = r'([ㄱ-ㅣ가-힣]+)(로)(제)(\d동)'
    pattern3 = r'([ㄱ-ㅣ가-힣]+)(제)(\d동)'

    if re.match(pattern1, name):
        return re.sub(pattern1, r'\1\2\4', name)
    elif re.match(pattern2, name):
        return re.sub(pattern2, r'\1\2\4', name)
    elif re.match(pattern3, name):
        return re.sub(pattern3, r'\1\3', name)
    else:
        return name


def convert_hdong_cd(code_df, code, accessToken):
    """
    - Arguments
        - code_df: 데이터 정의서 데이터프레임.
        - code: 검색 대상이 되는 행정동 코드.
        - accessToken: SGI API 액세스 토큰.
    - Purpose
        - 원본 csv 파일의 행정동 코드를 기준으로 시도명, 시군구명, 읍면동명 추출.
        - 추출한 장소명을 활용해서 API와 연동 가능한 행정동 코드로 다시 변환.
    - Returns
        - location: 시도명, 시군구명, 읍면동명을 합친 쿼리.
        - location_code_api: API 기준으로 업데이트된 행정동 코드.
    """
    ## CSV 행정동 코드로 매칭시켜서 주소 추출하기----------------------------------
    code_df = code_df[(code_df['행정동코드'] == code)].iloc[0]
    if code_df['시군구명'] == None:
        code_df['시군구명'] = ''
    if code_df['읍면동명'] == None:
        code_df['읍면동명'] = ''
    else:
        code_df['읍면동명'] = format_dong_name(code_df['읍면동명'])
    location = code_df['시도명']+" "+code_df['시군구명']+" "+code_df['읍면동명']
    ##--------------------------------------------------------------------
                     
    ## SGIS GeoCoding API로 타겟 주소 행정동 코드 받아오기------------------------
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/geocodewgs84.json"
    params = {
        "accessToken": accessToken,
        "address": location
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    try:
        request = urllib.request.Request(full_url)
        with urllib.request.urlopen(request) as response:
            response = json.loads(response.read().decode('utf-8'))
            if response['errCd'] == -1:
                location_code_api = '00000000'
            else:
                sido_cd = response['result']['resultdata'][0]['sido_cd']
                sgg_cd = response['result']['resultdata'][0]['sgg_cd'][2:]
                sgg_cd = '000' if sgg_cd == 'null' else sgg_cd
                adm_cd = response['result']['resultdata'][0]['adm_cd']
                adm_cd = '000' if adm_cd == 'null' else adm_cd[5:]
                location_code_api = int(sido_cd+sgg_cd+adm_cd)
            del response
    except Exception as e:
        print(f"SGIS geocoding API를 불러오는 도중 문제가 발생했습니다: {e},")
    ##--------------------------------------------------------------------
    
    return location, location_code_api


def extract_polygon(location_code_api, accessToken):
    """
    - Arguments
        - location_code_api: convert_hdong_cd 메소드로 얻은 API 기준 행정동 코드.
        - accessToken: SGI API 액세스 토큰.
    - Purpose
        - 업데이트한 행정동 코드를 입력값으로 받아 행정경계구역 API 입력값으로 전달.
    - Returns
        - coordinates: 행정경계구역을 나타내는 다면체의 좌표 집합.
    """
    ## SGIS 행정구역경계 API로 경계 폴리곤 받아오기--------------------------------
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/boundary/hadmarea.geojson"
    params = {
        "accessToken": accessToken,
        "year": 2023,
        "adm_cd": location_code_api,
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    try:
        request = urllib.request.Request(full_url)
        with urllib.request.urlopen(request) as response:
            response = json.loads(response.read().decode('utf-8'))
            coordinates = response['features'][0]['geometry']['coordinates'][0]
            del response
    except Exception as e:
        print(f"SGIS 행정구역경계 API를 불러오는 도중 문제가 발생했습니다: {e}")
    ##--------------------------------------------------------------------

    return coordinates


def select_data(config):
    """
    - Arguments
        - date: MMDD 형식으로 표현한 검색 날짜.
        - destination: 도착지 도로명 주소.
        - view_all_day: 정해진 날짜의 모든 경로 조회 옵션.
        - time: 경로 조회 시간대.
        - SGIS_consumer_key: SGIS API 토큰 호출용
        - SGIS_consumer_secret: SGIS API 토큰 호출용
    - Purpose
        - 검색 날짜와 목적지와 매칭되는 정보만 원본 데이터로부터 추출.
        - 모든 시간대 혹은 정해진 시간대의 데이터 필터링 후 데이터프레임 형태로 반환.
    - Returns
        - od_data: 추출한 데이터프레임.
        - destination_latitude: 목적지 위도.
        - destination_longitude: 목적지 경도.
        - destination_code_api: 목적지 행정동 코드 (API 버전).
        - accessToken: SGIS API 토큰.
    """
    print('========================================================================================================')
    print('데이터 필터링을 시작합니다.')
    date = config['date']
    destination = config['destination']
    view_all_day = config['view_all_day']
    time = config['time']
    SGIS_consumer_key = config['SGIS_consumer_key']
    SGIS_consumer_secret = config['SGIS_consumer_secret']

    ## SGIS API 액세스 토큰 받아오기-------------------------------------------
    print('========================================================================================================')
    print('SGIS API 액세스 토큰 받아오는 중...')
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/auth/authentication.json"
    params = {
        "consumer_key": SGIS_consumer_key,
        "consumer_secret": SGIS_consumer_secret
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    try:
        request = urllib.request.Request(full_url)
        with urllib.request.urlopen(request) as response:
            response = json.loads(response.read().decode('utf-8'))
            accessToken = response['result']['accessToken']
            del response
    except Exception as e:
        print(f"SGIS API 액세스 토큰을 받아오는 도중 문제가 발생했습니다: {e}")
    ##--------------------------------------------------------------------

    ## SGIS GeoCoding API로 목적지 행정동 정보 받아오기--------------------------
    print('========================================================================================================')
    print('SGIS GeoCoding API로 목적지 행정동 정보 받아오는 중...')
    url = "https://sgisapi.kostat.go.kr/OpenAPI3/addr/geocodewgs84.json"
    params = {
        "accessToken": accessToken,
        "address": destination
    }
    query_string = urllib.parse.urlencode(params)
    full_url = f"{url}?{query_string}"
    try:
        request = urllib.request.Request(full_url)
        with urllib.request.urlopen(request) as response:
            response = json.loads(response.read().decode('utf-8'))
            sido_nm = response['result']['resultdata'][0]['sido_nm']
            sgg_nm = response['result']['resultdata'][0]['sgg_nm']
            adm_nm = response['result']['resultdata'][0]['adm_nm']
            destination_code_api = int(response['result']['resultdata'][0]['adm_cd'])
            destination_longitude = float(response['result']['resultdata'][0]['x'])
            destination_latitude = float(response['result']['resultdata'][0]['y'])
            del response
    except Exception as e:
        print(f"SGIS geocoding API를 불러오는 도중 문제가 발생했습니다: {e}")
    ##--------------------------------------------------------------------

    ## CSV 행정동 코드 추출---------------------------------------------------
    print('========================================================================================================')
    print('SGIS API와 연동 가능한 행정동 코드를 받아오는 중...')
    code_df = pd.read_csv(config['dong_code_path'])
    try:
        destination_code_csv = code_df[(code_df['시도명'] == sido_nm) & 
                                    (code_df['시군구명'] == sgg_nm) & 
                                    (code_df['읍면동명'] == adm_nm)]['행정동코드'].iloc[0]
    except Exception as e:
        print('========================================================================================================')
        print("CSV로부터 목적지의 행정동 코드를 불러오는데 실패했습니다:")
        print(sido_nm+' '+sgg_nm+' '+adm_nm)
        print("API에서 전달받은 읍면동명과 정확하게 매칭되는 정보가 CSV 내부에 존재하지 않습니다.")
        print("데이터 정의서 CSV를 직접 확인하여 원하는 행정동의 코드를 복사, 붙여넣기 해주세요.")
        while True:
            destination_code_csv = input("행정동 코드 입력: ")
            if isinstance(destination_code_csv, str) and len(destination_code_csv) == 10:
                destination_code_csv = int(destination_code_csv[:-2])
                break
            else:
                print("입력값을 다시 확인해보세요.")
    destination_code_csv = int(destination_code_csv)
    del code_df
    ##---------------------------------------------------------------------

    ## 날짜, 도착지 기준 데이터 필터링과 시간 변환-----------------------------------
    print('========================================================================================================')
    print('사용자 설정에 맞춰 데이터 필터링 중...')
    od_file = os.path.join(config['od_data_path'], 'od_2023'+date+'_1.csv')
    od_data = pd.read_csv(od_file)
    od_data = od_data[od_data['dest_hdong_cd']==destination_code_csv]
    od_data['start_time'] = od_data['start_time'].apply(convert_to_time)
    od_data['end_time'] = od_data['end_time'].apply(convert_to_time)
    ##--------------------------------------------------------------------

    if view_all_day:
        od_data.reset_index(inplace=True, drop='index')
        print('========================================================================================================')
        print(f'{date[:2]}월 {date[2:]}일 데이터를 전부 불러옵니다.')
        return od_data, destination_longitude, destination_latitude, destination_code_api, accessToken
    
    ## 특정 시간대에만 해당하는 데이터 추출----------------------------------------
    time = convert_to_time(time)
    od_data = od_data[(od_data['start_time'] <= time) & (od_data['end_time'] >= time)]
    od_data.reset_index(inplace=True, drop='index')
    ##--------------------------------------------------------------------

    print('========================================================================================================')
    print(f'{date[:2]}월 {date[2:]}일 {time}시 데이터를 불러옵니다.')
    return od_data, destination_longitude, destination_latitude, destination_code_api, accessToken


def create_origin_coordinates(config, od_data, accessToken):
    """
    - Arguments
        - od_data: 출발지 좌표를 랜덤 생성해줄 데이터프레임.
        - accessToken: SGI API 액세스 토큰.
    - Purpose
        - 출발지 좌표를 랜덤 생성.
    - Returns
        - 데이터프레임 반환.
    """
    print('========================================================================================================')
    print('출발지 좌표 랜덤 샘플링을 시작합니다.')
    code_df = pd.read_csv(config['dong_code_path'])

    ## convert_hdong_cd 메소드로 API 기준 출발지 행정동 코드 가져오기---------------
    print('========================================================================================================')
    print('SGIS API 기준 출발지 행정동 코드 가져오는 중...')
    location_code_api = []
    location_list = []
    for code in tqdm(od_data['origin_hdong_cd']):
        location, code_api = convert_hdong_cd(code_df, code, accessToken)
        location_code_api.append(code_api)
        location_list.append(location)
    od_data['origin_hdong_cd_api'] = location_code_api
    od_data['origin_name'] = location_list
    ##--------------------------------------------------------------------
    
    ## extract_polygon 메소드로 행정구역경계 설정 및 좌표 랜덤 설정하기---------------
    origin_coordinates = []
    print('========================================================================================================')
    print('행정구역경계 추출 및 좌표 랜덤화 진행 중...')
    for code in tqdm(od_data['origin_hdong_cd_api']):
        if code == '00000000':
            random_point = np.array([0.0,0.0])
            origin_coordinates.append(random_point)
        else:
            coordinates = np.array(extract_polygon(code, accessToken))
            if coordinates.ndim == 3:
                polygon = Polygon(coordinates[0])
            else:
                polygon = Polygon(coordinates)
            random_point = pointpats.random.poisson(polygon, size=1)
            del polygon
            random_point = random_point[np.newaxis, :]
            random_point = convert_coordinates(random_point)
            origin_coordinates.append(random_point)
    od_data['origin_coordinates'] = [coord[0] for coord in origin_coordinates]
    ##--------------------------------------------------------------------
    
    print('========================================================================================================')
    print('출발지 좌표 랜덤 샘플링을 완료했습니다.')
    return od_data


def create_destination_coordinates(config,
                                   od_data,
                                   destination_longitude,
                                   destination_latitude,
                                   destination_code_api,
                                   accessToken):
    """
    - Arguments
        - select_data 메소드 리턴값 참고
        - probability: 축제 참여 확률.
    - Purpose
        - 축제 참여 확률에 따라 도착지 좌표를 랜덤 생성.
    - Returns
        - od_data: 축제 참여 확률에 따라 설정된 도착지 좌표를 포함한 데이터프레임.
    """
    print('========================================================================================================')
    print('축제 참여확률 기반으로 목적지 좌표 랜덤 샘플링을 시작합니다.')
    probability = config['probability']
    total = len(od_data)
    visit_y = int(total*probability)
    visit_n = total - visit_y

    ## SGIS 행정구역경계 API로 경계 폴리곤 받아오기--------------------------------
    coordinates = extract_polygon(destination_code_api, accessToken)
    ##--------------------------------------------------------------------

    ## 도착지 좌표 축제 방문 확률에 맞춰서 생성하기----------------------------------
    polygon = Polygon(coordinates)
    random_points = pointpats.random.poisson(polygon, size=visit_n)
    random_points = convert_coordinates(random_points)
    fixed_points = np.array([destination_latitude, destination_longitude])
    fixed_points = np.tile(fixed_points, [visit_y,1])
    if len(random_points) == 0:
        destination_coordinates = fixed_points
    elif len(fixed_points) == 0:
        destination_coordinates = random_points
    else:
        destination_coordinates = np.vstack([random_points, fixed_points])
    del random_points, fixed_points
    np.random.shuffle(destination_coordinates)
    od_data['destination_coordinates'] = [coord for coord in destination_coordinates]
    ##--------------------------------------------------------------------

    print('========================================================================================================')
    print('목적지 좌표 랜덤 샘플링을 완료했습니다.')
    return od_data


def collect_errors(config, od_data, col):
    """
    - Purpose
        - 코드 실행 중 origin_destination 에러값들 수집 및 저장.
        - 제대로 좌표가 수집된 행들만 반환.
    """
    error_directory = config['error_directory']
    if not os.path.exists(error_directory):
        os.makedirs(error_directory)

    ## origin_destination 에러값들 수집 및 저장, 정상 데이터 반환------------------
    if col == 'origin_hdong_cd_api':
        errors = od_data[od_data['origin_hdong_cd_api']=='00000000']
        if len(errors) != 0:
            print('========================================================================================================')
            print('origin_hdong_cd_api 에러 케이스를 수집합니다.')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_path = os.path.join(error_directory, f"errors_{timestamp}.csv")
            errors.to_csv(error_path, index=False)
            od_data = od_data[od_data['origin_hdong_cd_api'] != '00000000'].reset_index(drop=True)
    ##--------------------------------------------------------------------

    ## route 에러값들 수집 및 저장, 정상 데이터 반환------------------------------
    if col == 'route':
        errors = od_data[od_data['route'].isnull()]
        if len(errors) != 0:
            print('========================================================================================================')
            print('route 에러 케이스를 수집합니다.')
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            error_path = os.path.join(error_directory, f"errors_{timestamp}.csv")
            errors.to_csv(error_path, index=False)
            od_data = od_data[~od_data['route'].isnull()].reset_index(drop=True)
    ##--------------------------------------------------------------------

    return od_data


def create_paths(config, od_data):
    """
    - Arguments
        - od_data: 경로 생성 대상이 되는 데이터프레임.
    - Purpose
        - 사용자가 원하는 수만큼 경로를 샘플링하여 데이터프레임에 추가.
    - Returns
        - 데이터프레임 반환.
    """

    ## openrouteservice 쿼리 횟수 제한에 맞춰 기존 데이터프레임에서 랜덤 샘플링--------
    print('========================================================================================================')
    print(f"현재 데이터는 {len(od_data)}개 입니다.")
    print(f"openrouteservice의 일일 쿼리 제한은 2000 입니다.")
    while True:
        num_query = int(input("원하는 검색 횟수를 입력해주세요: "))
        if num_query > 2000:
            print("입력된 값이 너무 큽니다. openrouteservice의 제한은 2000입니다.")
        elif num_query < 1:
            print("검색 횟수는 1 이상이어야 합니다.")
        else:
            od_data = od_data.sample(n=num_query)
            break
    ##--------------------------------------------------------------------

    ## modal을 profile로 매핑하는 딕셔너리 생성----------------------------------
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
    ##--------------------------------------------------------------------

    ## 분당 40회 경로 조회----------------------------------------------------
    routes = []
    query_count = 0
    start_time = time.time()
    while int(time.strftime("%S", time.localtime(start_time))) != 0:
        time.sleep(0.1)
        start_time = time.time()
    print('========================================================================================================')
    print('openrouteservice API로부터 경로 데이터를 받아오는 중입니다.')
    for _, data in tqdm(od_data.iterrows()):
        coords = [data['origin_coordinates'][::-1],data['destination_coordinates'][::-1]]
        try:
            client = openrouteservice.Client(key=config['ors_token'])
            route = client.directions(coords, profile=modal_dict[data['modal']])
            route = route["routes"][0]['geometry']
            route = convert.decode_polyline(route)
        except openrouteservice.exceptions.ApiError as e:
            route = None
        routes.append(route)

        query_count += 1
        if query_count == 40:
            elapsed_time = time.time() - start_time
            if elapsed_time < 60:
                time.sleep(60 - elapsed_time)
            start_time = time.time()
            query_count = 0
    od_data['route'] = routes


    ##--------------------------------------------------------------------

    return od_data


def save(config, od_data):
    """
    - Purpose
        - 결과 데이터프레임 옵션에 맞게 저장.
    """
    coordinates_only = config['coordinates_only']
    save_dir = config['save_directory']
    name = config['destination'].replace(" ", "")
    date = config['date']
    prob = config['probability']
    num = len(od_data)
    if config['view_all_day']==False:
        time = config['time']
    else:
        time = 'ALL'
    middle_dir = str(name)+'_D'+str(date)+'_T'+str(time)+'_P'+str(prob)

    if coordinates_only:
        save_dir = os.path.join(save_dir, middle_dir, 'coordinates')
        file_name = 'coordinates.json'
        columns=['origin_name','origin_hdong_cd','dest_hdong_cd','origin_coordinates',
                 'destination_coordinates','start_time','end_time','gender','age','modal',
                 'origin_purpose','dest_purpose','od_dist_avg','od_duration_avg','od_cnts']
    else:
        save_dir = os.path.join(save_dir, middle_dir, 'routes')
        file_name = f'routes_{num}_samples.json'
        columns=['origin_name','origin_hdong_cd','dest_hdong_cd','origin_coordinates',
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
        
    if coordinates_only:
        print('========================================================================================================')
        print('출발지와 도착지 좌표 데이터를 저장했습니다.')
    else:
        print('========================================================================================================')
        print('경로 데이터를 저장했습니다.')


if __name__ == "__main__":
    args = parse_args()

    with open(args.config, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)

    if config['coordinates_only']:
        od_data, destination_longitude, destination_latitude, destination_code_api, accessToken = select_data(config)
        od_data = create_origin_coordinates(config, od_data, accessToken)
        od_data = create_destination_coordinates(config, od_data,
                                                destination_longitude,
                                                destination_latitude,
                                                destination_code_api,
                                                accessToken)
        od_data = collect_errors(config, od_data, col='origin_hdong_cd_api')
        save(config, od_data)
    else:
        od_data = pd.read_json(config['coordinates_path'])
        od_data = create_paths(config, od_data)
        od_data = collect_errors(config, od_data, col='route')
        save(config, od_data)