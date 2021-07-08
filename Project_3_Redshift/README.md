### Project description

The project creates a Redshift cluster, connects to a database in the cluster and stages and loads data into the database tables.

### How to use:

#### Step 1:
First add your AWS SECRET and KEY credentials to the config (dwh.cfg) file.

Use the notebook cluster_management.ipynb to manage the Redshift cluster (create/delete) along with the associated IAM role and policies.

Run section 1. of the cluster_management notebook to create the role and cluster. After you created the cluster you will need to copy the HOST and ARN variables into the config file and save it.

#### Step 2:
Then you can run create_tables.py and etl.py from the command line.

The former establishes a connection to Redshift's database called dwh, drops tables if they exist and creates new tables as per the sql_queries.py file (namely two staging_events, staging_songs, songplays, users, songs, artists and time).

The latter loads log_data and song_data into the two staging tables and then transforms them into the rest of the tables.

#### Step 3:
You can also use cluster_management.ipynb (section 2) to query the data after you have successfully created the database and loaded the data.

#### Step 4:
Finally, use cluster_management.ipynb (section 3) to delete the Redshift cluster and detach the asscociated role policies.