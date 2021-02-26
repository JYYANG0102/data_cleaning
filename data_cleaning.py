# encoding=utf-8
'''
DESC:
For the project: /Performance Calculation of Turbine/

The codes read the log files (*.csv) from the DCS of a power plant, and a log file (IMP_points.csv)
from the device of IMP, and a mapping file of the two kinds of log files. Then, we merge these DCS
or /IMP log files into just one the first output. And then, we extract those valid data points from
the log files above according to the mapping file, filter the data points into one value. Finally,
we create the output file, output.json.

The files tree:
input (dir) -> DCS (dir) -> *.csv
            -> IMP (dir) -> *.csv
            -> MAPPING (dir) -> mapping.csv
output (dir) -> output.json

In addition, we are able to packages the python source into a standalone Windows executables by
means of the command tool PyInstaller. eg. 'pyinstaller -F data_cleaning.py '

AUTHOR: Zhi-wei YAN (jerodyan@163.com)
DATE:   2020-12-29
VERSION：0.1
'''

import glob
import json
import os
import sys
from datetime import datetime
from time import sleep

import pandas as pd


# help message functions.
def hr(msg=""):
    print("")
    print(80 * '-')
    print(msg)
    sleep(0.5)


def print_usage():
    print('\"' * 80)
    print("The program deals with the CSV files (UTF-16 or UTF-8) in the dir (./input/) as inputs, ")
    print("writes a summary file 'output.json' into the dir (./output/) as an output. ")
    print("")
    print('** Warnning **: JUST FOR /ONE/ OPERATING CONDITION!! ')
    print('** Warnning **: WE DO NOT HANDLE THE TIMESTAMPS CROSS THE MIDDLE NIGHT!! ')
    print("")
    print('Copyright 2020, Rui-Dian-Sci-Tech. Contact: jerodyan@163.com.')
    print('\"' * 80)


def press_and_continue():
    a = input("Press ENTER key to continue.")


def press_and_exit(code):
    a = input("Press ENTER key to exit.")
    sys.exit(code)


# folder or directory functions.
def check_folder(dir_name):
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    print("Checking Dir: " + dir_name)
    return dir_name


def bad_point_length_encoding(data):
    encoding = []
    prev_char = ''
    count = 1

    if not data: return ''

    for char in data:
        # If the prev and current characters
        # don't match...
        if char != prev_char:
            # ...then add the count and character
            # to our encoding
            if prev_char:
                encoding.append({prev_char: str(count)})
            count = 1
            prev_char = char
        else:
            # Or increment our counter
            # if the characters do match
            count += 1
    else:
        # Finish off the encoding
        encoding.append({prev_char: str(count)})
        return encoding


def calibrate_clock_detla(data_src):
    # delta = 0.0
    # return delta
    try:
        delta = input(data_src + "Clock - PekingClock (seconds): ")
    except:
        delta = 0
    return delta


# Some Constants
input_files_extension_name = '*.csv'
input_dir_dcs_logs = './input/DCS/'
input_dir_imp_logs = './input/IMP/'
input_dir_mapping = './input/MAPPING'

input_dir_mapping_csv = './input/MAPPING/mapping.csv'

output_dir = './output/'
output_file = 'output.json'

# American Society of Mechanical Engineers
# recommend some constraints for the project.
asme_min_duration = 600
csv_encoding = ['utf_16', 'utf_8', 'gb18030', 'gb2312']

if __name__ == "__main__":
    print_usage()

    # check the input dir and the output dir.
    check_folder(input_dir_dcs_logs)
    check_folder(input_dir_imp_logs)
    check_folder(input_dir_mapping)

    check_folder(output_dir)

    # check and scan the input files
    hr('DCS Raw Data: ')
    t_delta = calibrate_clock_detla("DCS Raw Data")

    # DCS Raw Data with encoding utf_16
    dcs_files = sorted(glob.glob(input_dir_dcs_logs + input_files_extension_name))
    if len(dcs_files) == 0:
        print('ERROR: There are NOT %s files in the directory %s.' % (input_files_extension_name, input_dir_dcs_logs))
        # press_and_continue()
    else:
        print('There are [ %d ] log files in the directory %s' % (len(dcs_files), input_dir_dcs_logs))
        # print(dcs_files)

    dcs_sheets = pd.Series([])
    # custom_date_parser = lambda x: datetime.strptime(x, "\'%Y/%m/%d %H:%M:%S")
    dcs_timestamp_format = input("DCS Column Time Format (eg. %Y/%m/%d %H:%M:%S):")
    # dcs_timestamp_format =  "\'%Y/%m/%d %H:%M:%S"
    print("Your input is : %s\n" % dcs_timestamp_format)
    custom_date_parser = lambda x: datetime.strptime(x, dcs_timestamp_format)

    for (idx, sample_file) in enumerate(dcs_files):
        try:
            for item in csv_encoding:
                try:
                    print('trying to read the file [%d] %s with the encoding %s,' % (idx + 1, sample_file, item),
                          end=' ')
                    csv = pd.read_csv(sample_file, sep=r'\s*\t|\s*,', engine='python', encoding=item,
                                      squeeze=True, header=0,
                                      parse_dates=['Time'], date_parser=custom_date_parser)
                    print('\t[OK].')
                    break
                except:
                    print("\t[Failed].")
            # remove the column with NAN
            csv = csv.T.dropna()
            csv = csv.T
            csv.set_index('Time', inplace=True)
            csv = csv.resample('1S').ffill()
            # csv = csv.T
            # print('\n', csv)
        except:
            print(', [Failed]')
            print("  - ERROR: Opening the file: %s, and Skip the file." % (sample_file))
            print(sys.exc_info())
            continue

        # combine the records in all files into one dataframe.
        if dcs_sheets.empty:
            dcs_sheets = csv
        else:
            dcs_sheets = pd.merge(dcs_sheets, csv, on='Time')
            # dcs_sheets.index = dcs_sheets.index + pd.DateOffset(seconds=int(t_delta))
        # print(csv)
        sleep(0.3)

    if dcs_sheets.empty:
        hr('WARNING: There are not any records to be analyzed.')
        print('Sorry, It failed. You SHOULD check your .csv files again. ')
        press_and_exit(1)
    else:
        dcs_sheets.index = dcs_sheets.index + pd.DateOffset(seconds=int(t_delta))
        print(dcs_sheets)
        row, col = dcs_sheets.shape
        print("DCS Points Name Number: ", col)
        print("DCS Points Sampling Number (1 sample/1 sec): ", row)

    hr("IMP Raw Data: ");
    t_delta = calibrate_clock_detla("IMP Raw Data")
    # IMP data
    imp_files = sorted(glob.glob(input_dir_imp_logs + input_files_extension_name))
    if len(imp_files) == 0:
        print('ERROR: There are NOT %s files in the directory %s.' % (input_files_extension_name, input_dir_imp_logs))
        # press_and_continue()
    else:
        print('There are [ %d ] log files in the directory %s' % (len(imp_files), input_dir_imp_logs))
        # print(imp_files)

    imp_sheets = pd.Series([])
    # custom_date_parser = lambda x: datetime.strptime(x, "%d%H:%M:%S")
    # custom_date_parser = lambda x: datetime.strptime(x, "%H:%M:%S")
    imp_timestamp_format = input("IMP Column Time Format (eg. %Y/%m/%d %H:%M:%S):")
    # imp_timestamp_format = "%d %H:%M:%S"

    print("Your input is : %s\n" % imp_timestamp_format)
    custom_date_parser = lambda x: datetime.strptime(x, imp_timestamp_format)
    for (idx, sample_file) in enumerate(imp_files):
        try:
            for item in csv_encoding:
                try:
                    print('trying to read the file [%d] %s with the encoding %s,' % (idx + 1, sample_file, item),
                          end=' ')
                    csv = pd.read_csv(sample_file, sep=r'\s*\t|\s*,', engine='python', encoding=item,
                                      squeeze=True, header=0,
                                      parse_dates=['Time'], date_parser=custom_date_parser)
                    print('\t[OK].')
                    break
                except:
                    print("\t[Failed].")
            # remove the column with NAN
            csv = csv.T.dropna()
            csv = csv.T
            csv.set_index('Time', inplace=True)
            csv = csv.resample('1S').ffill()
        except:
            print(', [Failed]')
            print("  - ERROR: Opening the file: %s, and Skip the file." % (sample_file))
            print(sys.exc_info())
            continue

        # combine the records in all files.
        if imp_sheets.empty:
            imp_sheets = csv
        else:
            imp_sheets = pd.merge(imp_sheets, csv, on='Time')
        # print(csv)
        sleep(0.3)

    if imp_sheets.empty:
        hr('WARNING: There are not any records to be analyzed.')
        print('Sorry, It failed. You SHOULD check your .csv files again. ')
        press_and_exit(1)
    else:
        imp_sheets.index = imp_sheets.index + pd.DateOffset(seconds=int(t_delta))
        print(imp_sheets)
        row, col = imp_sheets.shape
        print("IMP Points Name Number: ", col)
        print("IMP Points Sampling Number (1 sample/1 sec): ", row)

    hr('Merged DCS and IMP Sheets: ')
    # # remove the year month and date from index 'Time'
    # imp_sheets.index = imp_sheets.index.time
    # dcs_sheets.index = dcs_sheets.index.time
    # imp_sheets.index.rename('Time', inplace=True)
    # dcs_sheets.index.rename('Time', inplace=True)

    merged_sheets = pd.merge(imp_sheets, dcs_sheets, on='Time', how='inner')
    print(merged_sheets)
    row, col = merged_sheets.shape
    print("Merged Points Name Number: ", col)
    print("Merged Points Sampling Number(1 sample/1 sec): ", row)

    hr("Mapping DCS/IMP Point Name To Json Name")
    try:
        mapping_csv = pd.read_csv(input_dir_mapping_csv, sep=r',\s*', engine='python',
                                  encoding='utf_8', squeeze=True, na_values=['Null', 'NULL', 'null'])
    except:
        mapping_csv = pd.read_csv(input_dir_mapping_csv, sep=r',\s*', engine='python',
                                  encoding='gb18030', squeeze=True, na_values=['Null', 'NULL', 'null'])
    hr('mapping_csv dataframe:')
    print(mapping_csv)

    hr('parsing mapping rule:')
    cleaned_sheets = pd.Series([])
    binary_cleaned_sheets = cleaned_sheets
    for row in mapping_csv.itertuples(index=False):
        if pd.isnull(row.imp_point_name) and pd.isnull(row.dcs_point_name):
            continue

        if not pd.isnull(row.imp_point_name):
            point_name = row.imp_point_name
            point_source = 'IMP'
        else:
            point_name = row.dcs_point_name
            point_source = 'DCS'

        json_key_name = row.json_key_name
        json_key_memo_cn = row.json_key_memo_CN

        if point_name not in merged_sheets.keys():
            print("\nERROR: %s-->%s<--%s not in DCS or IMP raw data set.\n" % (json_key_name,
                                                                               point_name,
                                                                               json_key_memo_cn))
            continue

        print(json_key_name, '\t->>\t', point_source, point_name, '\t<<-', json_key_memo_cn, end='')
        col = merged_sheets[point_name].copy()
        col.rename(json_key_name, inplace=True)

        if not pd.isnull(row.valid_interval):
            [bottom_limit, upper_limit] = row.valid_interval.split('-')
            print("\t\tlimited by [%s, %s)." % (bottom_limit, upper_limit))
            binary_col = col.map(lambda x: 1 if x < float(bottom_limit) or x >= float(upper_limit) else 0)
        else:
            binary_col = col.map(lambda x: 1 if pd.isnull(x) else 0)
            print("\t\tlimited by [ NULL ).")

        if cleaned_sheets.empty:
            cleaned_sheets = col
            binary_cleaned_sheets = binary_col
        else:
            cleaned_sheets = pd.merge(cleaned_sheets, col, on='Time', how='inner')
            binary_cleaned_sheets = pd.merge(binary_cleaned_sheets, binary_col, on='Time', how='inner')
    hr("filtered merged dataframe with threshold constraints: (1 means bad, 0 means good)")
    # print(cleaned_sheets)
    print(binary_cleaned_sheets)

    boolean_series = binary_cleaned_sheets.agg(sum, axis=1)
    # print(boolean_series)

    hr("filtered by bad points with timestamp constraints:")
    msg = boolean_series.map(lambda x: 'x' if x > 0 else 'o')
    msg = msg.str.cat(sep='')
    encoded_msg = bad_point_length_encoding(msg)
    print("bad points(x) and good points(o):")
    print(encoded_msg)

    x = encoded_msg
    head = 0
    tail = 0
    for i in range(len(x)):
        key = ''
        value = 0
        for k in x[i]:
            value = int(x[i][k])
            key = k

        tail = head + int(value)
        begin_timestamp = boolean_series.index[head]
        end_timestamp = boolean_series.index[tail - 1]
        if key == 'x':
            print(i, '*Delete* invalid data set with time interval: ', begin_timestamp, '-->', end_timestamp)
            cleaned_sheets = cleaned_sheets[
                cleaned_sheets.index.map(lambda t: t < begin_timestamp or t > end_timestamp)]
        else:
            print(i, '***Keep*** valid data set with time interval: ', begin_timestamp, '-->', end_timestamp)

        head = head + value
    print(cleaned_sheets)  # 删除坏点数据后所剩的df
    # hr("resample and fill date into the *Delete* time intervals. ")
    df = cleaned_sheets
    df['Datetime'] = pd.to_datetime(df.index.astype(str))
    df = df.set_index(pd.DatetimeIndex(df['Datetime']))
    final_sheets = df.resample('1S').ffill()  # TODO:这里将所有坏点的时间段的值进行了向上补齐，但并未判断剩余的好的时间是否大于一个小时
    print(final_sheets)

    result = final_sheets.median(axis=0).to_json(orient='index')
    parsed = json.loads(result)
    # print(parsed.keys())

    hr("json with valid values:")
    print(json.dumps(parsed, indent=4, sort_keys=True))
    json_obj = []
    for item, imp_unit, dcs_unit, memo_cn in mapping_csv[
        ['json_key_name', 'imp_point_metric_unit', 'dcs_point_metric_unit', 'json_key_memo_CN']].itertuples(
            index=False):
        if item in parsed.keys():
            # json_obj = {"value":parsed[item], "metric_unit":}
            # unit = lambda x:
            parsed[item] = {"value": parsed[item],
                            "metric_unit": imp_unit if pd.isnull(dcs_unit) else dcs_unit,
                            "alias_CN": memo_cn}
            print("Existed:\t %s\t %s = %s" % (item, memo_cn, parsed[item]))
        else:
            print("Non-Existed:\t %s\t %s." % (item, memo_cn))
            parsed[item] = None

    hr("for special rules:")
    print("water level at start or end:")
    water_level_list = ['dtr_water_level', 'cdsor_water_level']
    time_duration = 300
    for item in water_level_list:
        if item not in parsed.keys():
            print("Non-Existed:\t" + item)
            parsed[item + '_start'] = 'null'
            parsed[item + '_end'] = 'null'
            continue

        start = final_sheets[item].head(time_duration).median(axis=0)
        end = final_sheets[item].tail(time_duration).median(axis=0)
        parsed[item + '_start'] = {"value": start,
                                   "metric_unit": "mm",
                                   "alias_CN": "memo_cn"}
        parsed[item + '_end'] = {"value": end,
                                 "metric_unit": "mm",
                                 "alias_CN": "memo_cn"}
        print(item + '_start:', start)
        print(item + '_end:', end)

    with open(output_dir + output_file, 'w', encoding='utf-8') as fp:
        json.dump(parsed, fp, indent=4, sort_keys=True, ensure_ascii=False)

    hr("Output file: " + output_dir + output_file)
    hr("Data Cleaning Finished.")
    print('=' * 80)
