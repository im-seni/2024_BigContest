import uuid
import folium
import numpy as np
import pandas as pd
import geopandas as gpd
import branca.colormap as cm
from shapely.strtree import STRtree
from shapely.geometry import MultiLineString, LineString

class Map():

    def __init__(self, config):
        self.config = config
        self.center, self.geoDataFrame = self.calculate_intersections()
        self.map = self.draw_map()
    
    def calculate_intersections(self):
        
        od_data = pd.read_json(self.config['json_path'])
        center = od_data['destination_coordinates'].mode()[0][::-1]
        
        ## 문자열로 저장된 좌표계 LineString 객체로 변환
        linestrings = []
        for _, data in od_data.iterrows():
            linestrings.append(LineString(data['route']['coordinates']))
        od_data['route'] = linestrings

        ## LineString을 세그먼트로 쪼개기
        segments = []
        for line in linestrings:
            coords = list(line.coords)
            for i in range(len(coords) - 1):
                segment = LineString([coords[i], coords[i + 1]])
                segments.append(segment)

        ## 세그먼트 DataFrame 생성
        segments_df = pd.DataFrame(segments, columns=['segment'])
        unique_segments = segments_df['segment'].unique().tolist()

        ## 교차 카운트
        overlap_counts = [] 
        tree = STRtree(linestrings)
        for unique_segment in unique_segments:
            rep = 0
            indices = tree.query(unique_segment, predicate="intersects")
            for line in tree.geometries.take(indices):
                if isinstance(unique_segment.intersection(line), LineString):
                    row = od_data[od_data['route']==line]
                    rep+=row['od_cnts'].values[0]
                elif isinstance(unique_segment.intersection(line), MultiLineString):
                    row = od_data[od_data['route']==line]
                    rep+=row['od_cnts'].values[0]
            overlap_counts.append(rep)

        ## uuid 생성
        seg_id = []
        for unique_segment in unique_segments:
            id = str(uuid.uuid4())
            seg_id.append(id)

        ## 결과 GeoDataFrame 생성
        result_dict = {
            'uuid': seg_id,
            'geometry': unique_segments,
            'count': overlap_counts
        }
        result_df = gpd.GeoDataFrame(result_dict, crs="EPSG:4326")
        return center, result_df


    def draw_map(self):

        center, df = self.center, self.geoDataFrame
        map = folium.Map(location=center, zoom_start=10)
        colormap = cm.LinearColormap(["green", "yellow", "orange", "red"],
                                     vmin=df['count'].min(),
                                     vmax=df['count'].max())

        for _, row in df.iterrows():
            geometry = row['geometry']
            count = row['count']
            coords = list(geometry.coords)
            
            folium.PolyLine(
                locations=[(lat, lon) for lon, lat in coords],
                color=colormap(count),
                weight=self.config['PolyLine_weight'],
                opacity=self.config['PolyLine_opacity'],
                tooltip=folium.Tooltip(f'Count: {count}')
            ).add_to(map)

        # 칼러맵 패널 추가.
        colormap.add_to(map)

        ## 도착지 마커 추가.
        folium.Marker(
            location=center,
            popup=f"Destination",
            icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(map)

        # 다크 모드, 라이트 모드 추가.
        folium.TileLayer('cartodbdark_matter',name="dark mode",control=True).add_to(map)
        folium.TileLayer('cartodbpositron',name="light mode",control=True).add_to(map)
        folium.LayerControl(collapsed=False).add_to(map)
        return map
    