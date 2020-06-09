from flask import Flask, request, jsonify

from ffmpy import FFmpeg
from google.cloud import storage

import os


app = Flask(__name__)
UPLOAD_DIR = '/tmp/video_conversion'

app.config['UPLOAD_DIR'] = UPLOAD_DIR


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


@app.route('/convert', methods=['GET', 'POST'])
def convert_video():
    # if request.method == 'POST':
    #     # check if the post request has the file part
    #     if 'file' not in request.files:
    #         app.logger.error('No file submitted')
    #         return redirect(request.url)
    #
    #     file_to_convert = request.files['file']
    #     original = file_to_convert.filename
    #     app.logger.info('File received: %s', original)
    #     original_path = os.path.join(app.config['UPLOAD_DIR'], original)
    #     new_path = os.path.join(app.config['UPLOAD_DIR'], _new_filename(original))
    #     if file_to_convert:
    #         file_to_convert.save(original_path)
    #         new_file = _convert_to_mov(original_path, new_path)
    #         app.logger.info('Will return file: %s', os.path.basename(new_file))
    #         return send_from_directory(app.config['UPLOAD_DIR'], os.path.basename(new_file), as_attachment=True)
    #
    # return '''
    # <!doctype html>
    # <title>Upload MOV File to Convert</title>
    # <h1>Upload new File</h1>
    # <form method=post enctype=multipart/form-data>
    # <p><input type=file name=file>
    # <input type=submit value=Upload>
    # </form>
    # '''

    try:
        src_bucket_name = request.args.get('src_bucket_name')
        src_file_name = request.args.get('src_file_name', '', type=str)
        dest_bucket_name = request.args.get('dest_bucket_name', '', type=str)
        # dest_file_name = request.args.get('dest_file_name', '', type=str)

        if src_bucket_name != '' and src_file_name != '' and dest_bucket_name != '':
            # Init storage client
            storage_client = storage.Client.from_service_account_json('Google Cloud Run for FFMPEG-8a17770a848a.json')
            # storage_client = storage.Client()

            # Download original video
            src_bucket = storage_client.bucket(src_bucket_name)
            src_file = src_bucket.blob(src_file_name)
            src_file.download_to_filename(os.path.join(app.config['UPLOAD_DIR'], _new_filename(src_file_name)))

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


if __name__ == '__main__':
    app.run(host='0.0.0.0')
