from pathlib import Path
import zipfile
import os
import oss2
import shutil

def path_is_parent(parent_path, child_path):
    parent_path = os.path.abspath(parent_path)
    child_path = os.path.abspath(child_path)
    return os.path.commonpath([parent_path]) == os.path.commonpath([parent_path, child_path])

def is_ignored(target_path, ignores):
    for ignore in ignores:
        if os.path.relpath(target_path, ignore) == '.' or path_is_parent(ignore, target_path):
            return True
    return False

def zip_file(workspace, eve_app):
    os.chdir('%s/%s/code' % (workspace, eve_app))
    ignore_list = ['./.git', './.github', './.idea', './.DS_Store', './.vscode']
    with zipfile.ZipFile('code.zip', mode="w") as f:
        for dirpath, dirnames, filenames in os.walk('./'):
            if dirpath != './' and is_ignored(dirpath, ignore_list):
                continue
            for filename in filenames:
                absoult_file_path = os.path.join(dirpath, filename)
                if not is_ignored(absoult_file_path, ignore_list) and "code.zip" not in filename:
                    f.write(os.path.join(dirpath, filename))

    return 'code.zip'

def upload_oss(code_name, zip_file):
    auth = oss2.Auth(os.environ.get('AccessKeyId'), os.environ.get('AccessKeySecret'))
    bucket = oss2.Bucket(auth, os.environ.get('ArtifactEndpoint'), os.environ.get('ArtifactBucket'))

    with open(zip_file, 'rb') as fileobj:
        object_name = '%s/code.zip' % (code_name)
        bucket.put_object(object_name, fileobj)

workspace = os.getcwd()
with open('update.list') as f:
    publish_list = [eve_app.strip() for eve_app in f.readlines()]

failed_oss = []
for eve_app in publish_list:
    os.chdir(workspace)
    try:
        code_zip = zip_file(workspace, eve_app)
        upload_oss(eve_app, code_zip)
    except Exception as e:
        print('Failed to publish oss, app %s, err: %s' % (eve_app, e)) 
        failed_oss.append(eve_app)
    
print('Failed oss list: ', failed_oss)