from flask import Flask, request, jsonify

from ffmpy import FFmpeg
from google.cloud import storage
import googleapiclient.discovery

import os
import json
import glob
import cv2
import numpy as np


app = Flask(__name__)
UPLOAD_DIR = '/tmp/video_conversion'
UPLOAD_IMAGE_DIR = '/tmp/image_extraction'

app.config['UPLOAD_DIR'] = UPLOAD_DIR
app.config['UPLOAD_IMAGE_DIR'] = UPLOAD_IMAGE_DIR
app.config['BASE_DIR'] = os.path.abspath(os.path.dirname(__file__))
app.config['GOOGLE_KEY'] = 'Google Cloud Run for FFMPEG-8a17770a848a.json'


def _new_filename(filename):
    name, ext = os.path.splitext(filename)
    return f'{name}.mov'


def _new_img_filename(filename):
    name, ext = os.path.splitext(filename)
    return f'{name}.jpg'


def _convert_to_mov(original, new):
    app.logger.info('Converting %s -> %s', original, new)
    ff = FFmpeg(
        inputs={original: None},
        outputs={new: None}
    )
    ff.run()
    app.logger.info('Conversion successful: %s', new)
    return new


def transfer(description, project_id, start_date, start_time, source_bucket,
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


def _remove_tmp_dir(path):
    # remove all files from upload directory
    files = glob.glob(path)
    for f in files:
        try:
            os.remove(f)
        except Exception as e:
            print("Error: %s : %s" % (f, e.strerror))
            return False

    return True


@app.route('/convert', methods=['GET', 'POST'])
def convert_video():
    try:
        # remove all files from upload directory
        tmp_path = os.path.join(app.config['UPLOAD_DIR'], '*.*')
        _remove_tmp_dir(tmp_path)

        src_bucket_name = request.args.get('src_bucket_name')
        src_file_name = request.args.get('src_file_name', '', type=str)
        dest_bucket_name = request.args.get('dest_bucket_name', '', type=str)
        # dest_file_name = request.args.get('dest_file_name', '', type=str)

        if src_bucket_name != '' and src_file_name != '' and dest_bucket_name != '':
            # Init storage client
            storage_client = storage.Client.from_service_account_json(
                os.path.join(app.config.get('BASE_DIR'), app.config.get('GOOGLE_KEY')))

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


@app.route('/get_image', methods=['GET', 'POST'])
def get_image():
    try:
        tmp_path = os.path.join(app.config['UPLOAD_IMAGE_DIR'], '*.*')
        _remove_tmp_dir(tmp_path)

        src_bucket_name = request.args.get('src_bucket_name')
        src_file_name = request.args.get('src_file_name', '', type=str)
        src_file_fullname = os.path.join(app.config['UPLOAD_IMAGE_DIR'], src_file_name)
        dest_bucket_name = request.args.get('dest_bucket_name', '', type=str)

        if src_bucket_name != '' and src_file_name != '' and dest_bucket_name != '':
            # Init storage client
            storage_client = storage.Client.from_service_account_json(
                os.path.join(app.config.get('BASE_DIR'), app.config.get('GOOGLE_KEY')))

            # Download original video
            src_bucket = storage_client.bucket(src_bucket_name)
            src_file = src_bucket.blob(src_file_name)
            src_file.download_to_filename(src_file_fullname)

            # Extract non black image from video
            cam = cv2.VideoCapture(src_file_fullname)
            cur_frame = 0
            cnt = 0
            while True:
                ret, frame = cam.read()
                frame_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                frame_mean = np.mean(frame_gray)
                fm = cv2.Laplacian(frame_gray, cv2.CV_64F).var()
                if frame_mean >= 50 and fm >= 30:
                    cv2.imwrite(os.path.join(app.config['UPLOAD_IMAGE_DIR'], _new_img_filename(src_file_name)), frame)
                    break

                cnt += 1

            cam.release()
            cv2.destroyAllWindows()

            # Upload extracted image to storage
            dest_bucket = storage_client.bucket(dest_bucket_name)
            dest_file = dest_bucket.blob(_new_img_filename(src_file_name))
            dest_file.upload_from_filename(os.path.join(app.config['UPLOAD_IMAGE_DIR'], _new_img_filename(src_file_name)))

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
