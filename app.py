from flask import Flask, jsonify
import pandas as pd
import json


def get_processed_data():

    df = pd.read_csv('data.csv')
    df['source_address'] = df['source_host'] + ':' + df['source_port'].astype(str)  # Полные адреса источников
    df['receiver_address'] = df['receiver_host'] + ':' + df['receiver_port'].astype(str)  # Полные адреса получателей
    df['duration'] = df['end_time'] - df['start_time']  # Длительность передачи данных

    result_json = df.groupby(['receiver_address']).agg(
        receiver_address=pd.NamedAgg(column='receiver_address', aggfunc='max'),
        bytes_transferred=pd.NamedAgg(column='bytes_transferred', aggfunc='sum'),
        bytes_received=pd.NamedAgg(column='bytes_received', aggfunc='sum'),
        start_time=pd.NamedAgg(column='start_time', aggfunc='min'),
        end_time=pd.NamedAgg(column='end_time', aggfunc='max'),
        duration=pd.NamedAgg(column='duration', aggfunc='sum')
    ).to_json(orient='records')

    data = json.loads(result_json)
    data_json = json.dumps(data, indent=4)
    with open('my_results.json', 'w') as file:
        file.write(data_json)
    return data


app = Flask(__name__)
app.json.sort_keys = False


@app.route('/')
def test():
    return jsonify(get_processed_data())


if __name__ == '__main__':
    app.run()
