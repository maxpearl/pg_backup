import os
import sh
import click
from simple_AWS.s3_functions import *
from bu_globals import aws_profile, aws_region, local_tmp, backup_bucket

@click.command()
@click.option('--user', type=str, help="Postgresql Username")
@click.option('--password', type=str, help="Postgresql Password")
@click.option('--database', type=str, help="Postgresql Database")

def backup(user, password, database):
    """
    Back up PG database, copy to S3
    """
    # backup
    s3_file_name = f"{database}.psql"
    db_dump = sh.pg_dump("-U", f"{user}","-w", f"{database}")
    
    # upload to S3
    s3simple = S3Simple(region_name=aws_region, profile=aws_profile, bucket_name=backup_bucket)
    s3simple.put_to_s3(key=s3_file_name, body=str(db_dump))
    
if __name__ == '__main__':
    backup()



