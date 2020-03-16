import os
import sh
import click
import datetime
from simple_AWS.s3_functions import *
from bu_globals import aws_profile, aws_region, backup_bucket, tmp

@click.command()
@click.option('--user', type=str, help="Postgresql Username")
@click.option('--database', type=str, help="Postgresql Database")
@click.option('--date_suffix', is_flag=True, help="Add a date suffix to file")
@click.option('--config_file', type=str, help="Backup a config file")
@click.option('--config_dir', type=str, help="Backup a config directory")

def backup(user, database, date_suffix, config_file, config_dir):
    """
    Back up PG database, copy to S3
    """
    now = str(datetime.datetime.now())
    # backup
    if date_suffix:
        s3_file_name = f"{database}-{now}.psql"
    else:
        s3_file_name = f"{database}.psql"
    db_dump = sh.pg_dump("-U", f"{user}","-w", f"{database}")
    
    # upload to S3
    s3simple = S3Simple(region_name=aws_region, profile=aws_profile, bucket_name=backup_bucket)
    s3simple.put_to_s3(key=s3_file_name, body=str(db_dump))

    # config file or dir?
    if config_file:
        file_parts = config_file.split('/')
        s3_name = file_parts[-1]
        s3simple.send_file_to_s3(local_file=config_file, s3_file=s3_name)

    if config_dir:
        path_parts = config_dir.split('/')
        s3_name = 'backup' + '_'.join(path_parts) + '.zip'
        zip_name = tmp + s3_name
        result = sh.zip("-r", zip_name, config_dir)

        s3simple.send_file_to_s3(local_file=zip_name, s3_file=s3_name)

    
if __name__ == '__main__':
    backup()



