import os
import mimetypes
import paste.fileapp
from ckantoolkit import config

import ckantoolkit as toolkit
import ckan.logic as logic
import ckan.lib.base as base
import ckan.model as model
import ckan.lib.uploader as uploader
from ckan.common import _, request, c, response

from ckanext.archiver.tasks import archiverDownloader
import webob

import logging
log = logging.getLogger(__name__)

NotFound = logic.NotFound
NotAuthorized = logic.NotAuthorized
get_action = logic.get_action
abort = base.abort
redirect = toolkit.redirect_to


class ArchiverController(base.BaseController):

    def archive_download(self, id, resource_id, filename=None):
        '''
        Provide a download by either redirecting the user to the url stored or
        downloading the uploaded file from S3.
        '''
        context = {'model': model, 'session': model.Session,
                   'user': c.user or c.author, 'auth_user_obj': c.userobj}

        try:
            rsc = get_action('resource_show')(context, {'id': resource_id})
            get_action('package_show')(context, {'id': id})
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % id)

        if rsc.get('url_type') == 'upload':
            upload = uploader.get_resource_uploader(rsc)
            bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')
            region = config.get('ckanext.s3filestore.region_name')
            host_name = config.get('ckanext.s3filestore.host_name')
            bucket = upload.get_s3_bucket(bucket_name)

            if filename is None:
                filename = os.path.basename(rsc['url'])
            key_path = upload.get_path(rsc['id'], filename)
            key = filename

            if key is None:
                log.warn('Key \'{0}\' not found in bucket \'{1}\''
                         .format(key_path, bucket_name))

            try:
                # Small workaround to manage downloading of large files
                # We are using redirect to minio's resource public URL
                s3 = upload.get_s3_session()
                client = s3.client(service_name='s3', endpoint_url=host_name)

                # check whether the object exists in S3
                client.head_object(Bucket=bucket_name, Key=key_path)

                url = client.generate_presigned_url(ClientMethod='get_object',
                                                    Params={'Bucket': bucket.name,
                                                            'Key': key_path},
                                                    ExpiresIn=60)
                redirect(url)

            except ClientError as ex:
                if ex.response['Error']['Code'] in ['NoSuchKey', '404']:
                    # attempt fallback
                    if config.get(
                            'ckanext.s3filestore.filesystem_download_fallback',
                            False):
                        log.info('Attempting filesystem fallback for resource {0}'
                                 .format(resource_id))
                        url = toolkit.url_for(
                            controller='ckanext.s3filestore.controller:S3Controller',
                            action='filesystem_resource_download',
                            id=id,
                            resource_id=resource_id,
                            filename=filename)
                        redirect(url)

                    abort(404, _('Resource data not found'))
                else:
                    raise ex
