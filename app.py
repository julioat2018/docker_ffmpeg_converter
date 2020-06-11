from flask import Flask, request, jsonify

from ffmpy import FFmpeg
from google.cloud import storage
import googleapiclient.discovery

import os
import json
import glob


app = Flask(__name__)
UPLOAD_DIR = '/tmp/video_conversion'

app.config['UPLOAD_DIR'] = UPLOAD_DIR
app.config['BASE_DIR'] = os.path.abspath(os.path.dirname(__file__))
app.config['GOOGLE_KEY'] = 'Google Cloud Run for FFMPEG-8a17770a848a.json'


def _new_filename(filename):
    name, ext = os.path.splitext(filename)
    return f'{name}.mp4'


def _convert_to_mov(original, new):
    app.logger.info('Converting %s -> %s', original, new)
    ff = FFmpeg(
        inputs={original: None},
        outputs={new: None}
    )
    ff.run()
    app.logger.info('Conversion successful: %s', new)
    return new


def main(description, project_id, start_date, start_time, source_bucket,
         sink_bucket):
    """Create a daily transfer from Standard to Nearline Storage class."""
    storagetransfer = googleapiclient.discovery.build('storagetransfer', 'v1')

    # Edit this template with desired parameters.
    transfer_job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': start_date.day,
                'month': start_date.month,
                'year': start_date.year
            },
            'startTimeOfDay': {
                'hours': start_time.hour,
                'minutes': start_time.minute,
                'seconds': start_time.second
            }
        },
        'transferSpec': {
            'gcsDataSource': {
                'bucketName': source_bucket
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            },
            'objectConditions': {
                'minTimeElapsedSinceLastModification': '2592000s'  # 30 days
            },
            'transferOptions': {
                'deleteObjectsFromSourceAfterTransfer': 'true'
            }
        }
    }

    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))


@app.route('/convert', methods=['GET', 'POST'])
def convert_video():
    try:
        # remove all files from upload directory
        files = glob.glob(os.path.join(app.config['UPLOAD_DIR'], '*.*'))
        for f in files:
            try:
                os.remove(f)
            except Exception as e:
                print("Error: %s : %s" % (f, e.strerror))
                return jsonify([{
                    'result': "Error: %s : %s" % (f, e.strerror)
                }])

        src_bucket_name = request.args.get('src_bucket_name')
        src_file_name = request.args.get('src_file_name', '', type=str)
        dest_bucket_name = request.args.get('dest_bucket_name', '', type=str)
        # dest_file_name = request.args.get('dest_file_name', '', type=str)

        if src_bucket_name != '' and src_file_name != '' and dest_bucket_name != '':
            # Init storage client
            storage_client = storage.Client.from_service_account_json(
                os.path.join(app.config.get('BASE_DIR'), app.config.get('GOOGLE_KEY')))
            # storage_client = storage.Client()

            # Download original video
            src_bucket = storage_client.bucket(src_bucket_name)
            src_file = src_bucket.blob(src_file_name)
            src_file.download_to_filename(os.path.join(app.config['UPLOAD_DIR'], src_file_name))

            _convert_to_mov(os.path.join(app.config['UPLOAD_DIR'], src_file_name),
                            os.path.join(app.config['UPLOAD_DIR'], _new_filename(src_file_name)))

            # Upload converted video to storage
            dest_bucket = storage_client.bucket(dest_bucket_name)
            dest_file = dest_bucket.blob(_new_filename(src_file_name))
            dest_file.upload_from_filename(os.path.join(app.config['UPLOAD_DIR'], _new_filename(src_file_name)))

            return jsonify([{
                'result': 'success'
            }])
        else:
            return jsonify([{
                'result': 'Argument is required'
            }])
    except Exception as e:
        print(e)
        return jsonify([{
            'result': str(e)
        }])


@app.route('/test', methods=['GET', 'POST'])
def test():
    # remove all files from upload directory
    files = glob.glob('/tmp/video_conversion/*.*')
    for f in files:
        try:
            os.remove(f)
        except Exception as e:
            print("Error: %s : %s" % (f, e.strerror))
            return jsonify([{
                'result': "Error: %s : %s" % (f, e.strerror)
            }])

    return jsonify([{
        'result': 'success'
    }])

if __name__ == '__main__':
    app.run(host='0.0.0.0')
