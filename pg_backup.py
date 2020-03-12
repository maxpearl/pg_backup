import os
import sh
import click
import datetime
from simple_AWS.s3_functions import *
from bu_globals import aws_profile, aws_region, backup_bucket

@click.command()
@click.option('--user', type=str, help="Postgresql Username")
@click.option('--password', type=str, help="Postgresql Password")
@click.option('--database', type=str, help="Postgresql Database")
@click.option('--date_suffix', is_flag=True, help="Add a date suffix to file")

def backup(user, password, database, date_suffix):
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
    
if __name__ == '__main__':
    backup()



