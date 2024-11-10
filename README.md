# Route Simulator

대한민국 통계청 SGIS API와 Open Route Service API를 활용한 경로 시뮬레이터

## Author

- [정회수 (Hoesu Chung)](https://github.com/Hoesu)

## Environment
콘다 가상환경 설정
```bash
  conda create -n bigcon python=3.9.19
  conda activate bigcon
```
깃허브 풀 리퀘스트
```bash
  git init
  git remote add origin https://github.com/Hoesu/route_simulator.git
  git branch -m main
  git pull origin main
```
필수 라이브러리 설치
```bash
  pip install -r requirements.txt
```

## File Structure
```bash
Working Directory'
├── archive/
├── dataset/
│	  ├── od_data/
│   │   ├── od_20230901_1.csv
│   │   ├── ...
│	  │   └── od_20231015_1.csv
│	  └── data_info/
│		    └── reference.json
├── utils
│	  ├── main.py
│	  └── visualizer.py
├── configs
│	  ├── router_config.yaml
│	  └── visualizer_config.yaml
│
├── visualizer.ipynb
├── requirements.txt
├── README.md
└── .gitignore
```

## Data Pipeline
![1](https://github.com/user-attachments/assets/284206a8-ac55-4383-bc94-4fa630b3cffb)

경로 생성을 위해 첫 단계로 행정동 기준의 출발지와 도착지 좌표를 확인합니다. 이를 위해 SGIS API를 사용하여 행정동 경계 좌표를 얻고, KIKmix_20230701.csv의 행정동 데이터를 매칭하여 레퍼런스 자료를 만듭니다. API와 호환되는 행정동 코드를 통해 경계 좌표를 추출하고, 최종 결과를 json 파일로 저장하여 이후 과정에서 참고 자료로 활용합니다.

![2](https://github.com/user-attachments/assets/83112eab-239c-44eb-b76d-1920cf4105b6)

레퍼런스 자료가 완성되면 경로 생성을 본격적으로 시작할 수 있습니다. 먼저 router_config.yaml은 사용자 설정을 담은 파일이며, main.py는 이 설정 파일과 od_data, reference.json을 활용해 경로 생성 작업을 수행하는 실행 스크립트입니다. 메인 스크립트는 총 3가지 모드를 갖추고 있습니다. 첫 번째 모드 Coordinates Only는 주어진 출발·도착 행정동에 랜덤 좌표를 지정해 coordinates.json에 저장하고, 두 번째 모드 Routes Only는 coordinates.json을 기반으로 경로를 생성하여 routes.json에 저장합니다. 마지막으로 Fixed Origins는 사용자 지정 좌표를 입력으로 받아 fixed_routes.json에 경로를 저장하는 특별 모드입니다.

![3](https://github.com/user-attachments/assets/519971bb-8def-4bf7-90bd-f535fb1c1a35)

Coordinates Only 모드는 od_data를 필터링하여 출발지와 도착지 좌표를 생성하고 저장하는 네 단계로 이루어져 있습니다. 먼저 사용자가 설정한 조건에 맞는 데이터를 필터링한 후, 출발지와 도착지 행정동 좌표를 랜덤 샘플링으로 생성합니다. 설정한 비율에 따라 도착지 좌표를 특정 위치로 고정하거나 랜덤하게 배정할 수 있으며, 최종 좌표 데이터를 저장합니다.

![4](https://github.com/user-attachments/assets/ab0303ef-42ec-45f8-981d-b51934fa0fd4)

Routes Only 모드는 총 네 단계로 구성됩니다. 먼저 Coordinates Only 모드 결과를 불러오고, Open Route Service API를 사용해 출발-도착 좌표 간 경로를 생성합니다. 경로 연결이 어려운 경우에는 Route Snapping 기능으로 조정하며, 이동 수단도 지정할 수 있습니다. 마지막으로 에러 로그를 수집하고 생성된 경로를 저장합니다.

![5](https://github.com/user-attachments/assets/2d90e46c-50dd-43ef-95c3-122fa07f7e2e)

Fixed Origins 모드는 총 네 단계로 구성됩니다. 미리 설정한 출발지와 도착지 좌표를 불러온 뒤, Open Route Service API로 경로를 생성하며, Routes Only 모드와 동일하게 Route Snapping을 적용합니다. 생성 과정에서 발생한 에러를 수집하고, 최종 경로를 저장합니다. 이후, 생성된 경로의 과밀 구간을 추출하고 시각화하는 단계가 남아있습니다.

![6](https://github.com/user-attachments/assets/c51c4529-42d1-47a5-ac5d-1215c3b8b2bb)

경로 추출이 완료되면 교차 영역을 파악해야 하며, 이를 위해 경로 데이터를 Shapely의 LineString 객체로 변환합니다. 경로를 직선 단위로 분할하여 Segments 데이터프레임에 저장하고, od count 정보를 상속합니다. 이후, 중복 제거한 Unique Segments 데이터프레임과 Shapely의 STRtree 객체로 교차 작업을 효율화합니다. 겹치는 영역이 선인 경우만 교차로 인정하며, 결과는 교차 횟수를 담은 Results 데이터프레임에 저장됩니다.

## Router Configurations
```bash
## 모드 선택 옵션
## [C], [coordinates_only: True,  fixed_origins: False]: 사용자 설정에 따라 출발지, 도착지 좌표 설정만 하고 싶을때 선택.
## [R], [coordinates_only: False, fixed_origins: False]: 좌표는 이미 뽑아둔 상태에서 경로만 생성하고 싶을때 선택.
## [F], [coordinates_only: False, fixed_origins: True ]: 출발지를 정해놓은 상태에서 경로를 생성하고 싶을때 선택.
coordinates_only:   False
fixed_origins_only: False

## API 액세스 토큰 [CRF]
SGIS_consumer_key: ''
SGIS_consumer_secret: ''
ors_token: ''

## 파일 구조 옵션 [CRF]
od_data_path: './dataset/od_data'                         ## od_data 디렉토리
stay_data_path: './dataset/stay_data'                     ## stay_data 디렉토리
dong_code_path: './dataset/data_info/reference.json'      ## 행정동 코드, 경계 조회를 위한 json 파일 경로
save_directory: './results'                               ## 결과 저장용 디렉토리
error_directory: './errors'                               ## 에러 케이스 저장용 디렉토리

## 데이터 전처리 옵션 [CR]
date: ''                                                  ## 검색 날짜 ('MMDD')
destination: ''                                           ## 검색 목적지 도로명주소
view_all_day: True                                        ## 하루종일 보기 옵션
time: ''                                                  ## 특정 시간대만 보기 옵션 ('HH:MM')

## 좌표 생성 옵션 [CR]
probability: 0.8                                          ## 목적지 도착 확률 (0~1)

## 경로생성 대상 데이터파일 지정 [R]
coordinates_path: ''

## 지정좌표 기반 경로생성 옵션 [F]
fixed_origins:                                            ## 사용자 설정 출발지들의 경도, 위도를 담은 2차원 배열
fixed_destination:                                        ## 사용자 설정 목적지의 경도, 위도를 담은 1차원 배열
profile: 'driving-car'                                    ## 'driving-car', 'driving-hgv', 'cycling-regular'
                                                          ## 'cycling-road', 'cycling-mountain', 'cycling-electric'
                                                          ## 'foot-walking', 'foot-hiking', 'wheelchair'
```

## Visualizer Configurations
```bash
## 경로 json 상대경로
json_path: ''

## 경로 선 굵기
PolyLine_weight: 4

## 경로 선 불투명도
PolyLine_opacity: 1
```

## Deployment

```bash
  python utils/main.py -c configs/router_config.yaml
```

## Visualization
![7](https://github.com/user-attachments/assets/aa755c41-9ac9-4337-807e-79f525796b14)