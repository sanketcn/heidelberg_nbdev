import json
import base64
import subprocess
import os
import sys
from datetime import datetime
import time

import boto3

print(f'boto3 version: {boto3.__version__}')

try:
    sm = boto3.Session().client(service_name='sagemaker')
    sm_fs = boto3.Session().client(service_name='sagemaker-featurestore-runtime')
except:
    print(f'Failed while connecting to SageMaker Feature Store')
    print(f'Unexpected error: {sys.exc_info()[0]}')


# Read Environment Vars
FEATURE_GROUP = os.environ['FEATURE_GROUP_NAME']

features = ['kst_brutto', 'sm', 'tm', 'cl', 'so3', 'k2o', 'na2o', 'south_kiln_feed_01om886__tph__avg', 'south_kiln_feed_01om886__tph__max', 'north_kiln_feed_01om885__tph__avg', 'north_kiln_feed_01om885__tph__max', 'north_fan_speed_01oa943__rpm__avg', 'north_fan_speed_01oa943__rpm__max', 'south_fan_speed_02oa943__rpm__avg', 'south_fan_speed_02oa943__rpm__max', 'lignite_main_burner_03sk820__tph__avg', 'lignite_main_burner_03sk820__tph__max', 'bpg_main_burner_03bf810__tph__avg', 'bpg_main_burner_03bf810__tph__max', 'lignite_calciner_02sk820__tph__avg', 'lignite_calciner_02sk820__tph__max', 'bpg_calciner_02bf810__tph__avg', 'bpg_calciner_02bf810__tph__max', 'kbs_calciner_00kb950__tph__avg', 'kbs_calciner_00kb950__tph__max', 'total_energy_to_main_burner__gj_h__avg_0', 'total_energy_to_main_burner__gj_h__max_0', 'total_energy_to_main_burner__gj_h__avg_1', 'total_energy_to_main_burner__gj_h__max_1', 'total_energy_to_main_burner__gj_h__avg_2', 'total_energy_to_main_burner__gj_h__max_2', 'total_energy_to_main_burner__gj_h__avg_3', 'total_energy_to_main_burner__gj_h__max_3', 'total_energy_to_calciner__gj_h__avg_0', 'total_energy_to_calciner__gj_h__max_0', 'total_energy_to_calciner__gj_h__avg_1', 'total_energy_to_calciner__gj_h__max_1', 'total_energy_to_calciner__gj_h__avg_2', 'total_energy_to_calciner__gj_h__max_2', 'total_energy_to_calciner__gj_h__avg_3', 'total_energy_to_calciner__gj_h__max_3', 'tf__traditional_fuel__avg', 'tf__traditional_fuel__max', 'af__alternative_fuel__avg', 'af__alternative_fuel__max', 'shc__gj__t_kiln_feed__avg', 'shc__gj__t_kiln_feed__max', 'nox_concentration_in_kiln_inlet_00oa983__ppm__avg_0', 'nox_concentration_in_kiln_inlet_00oa983__ppm__max_0', 'nox_concentration_in_kiln_inlet_00oa983__ppm__avg_1', 'nox_concentration_in_kiln_inlet_00oa983__ppm__max_1', 'nox_concentration_in_kiln_inlet_00oa983__ppm__avg_2', 'nox_concentration_in_kiln_inlet_00oa983__ppm__max_2', 'nox_concentration_in_kiln_inlet_00oa983__ppm__avg_3', 'nox_concentration_in_kiln_inlet_00oa983__ppm__max_3', 'co_concentration_in_kiln_inlet_00oa981__ppm__avg', 'co_concentration_in_kiln_inlet_00oa981__ppm__max', 'o2_concentration_in_kiln_inlet_00oa982_____avg', 'o2_concentration_in_kiln_inlet_00oa982_____max', 'bottom_cyclone_gas_temperature_00oa800___c___avg', 'bottom_cyclone_gas_temperature_00oa800___c___max', 'secondary_air_temperature_01kk827___c___avg_0', 'secondary_air_temperature_01kk827___c___max_0', 'secondary_air_temperature_01kk827___c___avg_1', 'secondary_air_temperature_01kk827___c___max_1', 'secondary_air_temperature_01kk827___c___avg_2', 'secondary_air_temperature_01kk827___c___max_2', 'secondary_air_temperature_01kk827___c___avg_3', 'secondary_air_temperature_01kk827___c___max_3', 'tertiary_air_temperature_01kb801___c___avg_0', 'tertiary_air_temperature_01kb801___c___max_0', 'tertiary_air_temperature_01kb801___c___avg_1', 'tertiary_air_temperature_01kb801___c___max_1', 'tertiary_air_temperature_01kb801___c___avg_2', 'tertiary_air_temperature_01kb801___c___max_2', 'tertiary_air_temperature_01kb801___c___avg_3', 'tertiary_air_temperature_01kb801___c___max_3', 'kiln_amps_01do812__amps__avg_0', 'kiln_amps_01do812__amps__max_0', 'kiln_amps_01do812__amps__avg_1', 'kiln_amps_01do812__amps__max_1', 'kiln_amps_01do812__amps__avg_2', 'kiln_amps_01do812__amps__max_2', 'kiln_amps_01do812__amps__avg_3', 'kiln_amps_01do812__amps__max_3', 'co_bottom_cyclone_north_00oa941__amps__avg', 'co_bottom_cyclone_north_00oa941__amps__max', 'o2_bottom_cyclone_north_00oa942_____avg_0', 'o2_bottom_cyclone_north_00oa942_____max_0', 'o2_bottom_cyclone_north_00oa942_____avg_1', 'o2_bottom_cyclone_north_00oa942_____max_1', 'o2_bottom_cyclone_north_00oa942_____avg_2', 'o2_bottom_cyclone_north_00oa942_____max_2', 'o2_bottom_cyclone_north_00oa942_____avg_3', 'o2_bottom_cyclone_north_00oa942_____max_3', 'kiln_speed_01do811__rpm__avg', 'kiln_speed_01do811__rpm__max', 'primary_air_amount_01of850__nm3_h___avg', 'primary_air_amount_01of850__nm3_h___max', 'axial_air_pressure_01of811__mbar__avg', 'axial_air_pressure_01of811__mbar__max', 'radial_air_pressure_01of812__mbar__avg', 'radial_air_pressure_01of812__mbar__max', 'central_air_pressure_01of813__mbar__avg', 'central_air_pressure_01of813__mbar__max', 'total_feed_avg', 'total_feed_max', 'total_energy_avg', 'total_energy_max', 'average_fan_speed_avg', 'average_fan_speed_max', 'average_air_temperature_avg', 'average_air_temperature_max', 'kiln_feed_rate_avg', 'kiln_feed_rate_max', 'klinker___nk_c3s_lag_shift', 'klinker___nk_mgo_lag_shift', 'klinker___nk_fe2o3_lag_shift', 'klinker___nk_al2o3_lag_shift', 'klinker___nk_na2o_lag_shift', 'klinker___nk_k2o_lag_shift', 'klinker___nk_cao_lag_shift', 'klinker___nk_sio2_lag_shift', 'heat_of_formation__kcal_kg_clinker__lag_shift', 'klinker___nk_so3_lag_shift', 'klinker___nk_sm_lag_shift', 'klinker___nk_am_lag_shift', 'klinker___nk_lsf_lag_shift', 'id']

def update_agg(fg_name, data):
    record = []
    for feature in features:
        temp = {'FeatureName': feature, 'ValueAsString' : str(data[feature])}
        record.append(temp)
    sm_fs.put_record(FeatureGroupName=fg_name, Record=record)
    return
        
def lambda_handler(event, context):
    inv_id = event['invocationId']
    app_arn = event['applicationArn']
    records = event['records']
    print(f'Received {len(records)} records, invocation id: {inv_id}, app arn: {app_arn}')
    
    ret_records = []
    for rec in records:
        raw_data = rec['data']
        data_str = base64.b64decode(raw_data) 
        data = json.loads(data_str)
        print(f' updating features for id: {data}')
        update_agg(FEATURE_GROUP, data)
        
        # Flag each record as being "Ok", so that Kinesis won't try to re-send 
        ret_records.append({'recordId': rec['recordId'],
                            'result': 'Ok'})
    return {'records': ret_records}