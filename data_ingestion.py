def ingest_data_from_s3(current_working, s3, s3_path):
    try:
    """
    This function is used to Ingest data from Public S3 bucket site using s3fs library and store inside Landing Folder of Current working Directory. 
    Parameters:
    current_working: receive the current working directory to save data ex:- current_working_directory/Landing
    s3: S3 object for Public s3_bucket.
    s3_path: s3 bucket path from which we have to ingest data. 

    returns: None
    """

    # current_working = os.getcwd()
    # print(current_working)
    # s3 = s3fs.S3FileSystem(anon =  True)
    s3.get(f'{s3_path}', f'{current_working}/Landing1/',recursive=True, maxdepth=None)