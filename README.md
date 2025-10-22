# Big-Data-Mastodon

## How To use this project? 

### 0. Project requirements (Mint/Debian/Ubuntu)

`sudo apt update`

`sudo apt install -y postgresql postgresql-contrib python3-pip docker.io`

### 1. Kafka

Using `https://kafka.apache.org/quickstart` download and extract kafka.

### 2. Spark Streaming

Using `https://spark.apache.org/downloads.html` download and extract spark.

### 3. Python Environment

- Create your `/venv` using `python3 -m venv venv` and to activate it use `source venv/bin/activate`
- Using `pip install` command and `requirement-analytics.txt` install the required packages.
- Also  run `python -m textblob.download_corpora` while in `(venv)` environment. 

Example (spoonfeeding): 

For Simple: 
`pip install kafka-python mastodon.py pyspark psycopg2-binary sqlalchemy`

For ML: 
`pip install scikit-learn networkx pandas numpy textblob joblib`

Textblob: 
`python -m textblob.download_corpora`

### 4. Setting up schema (for Grefana Visualization)

Use commands below to make user, database and use `schema.sql` from `./database_scripts`

`sudo -u postgres psql <<EOF`

`CREATE DATABASE mastodon_analytics;`

`CREATE USER mastodon WITH PASSWORD 'your_password';`

`GRANT ALL PRIVILEGES ON DATABASE mastodon_analytics TO mastodon;`

`\c mastodon_analytics`

`\i schema.sql`

`EOF`

### 5. Running the Project

Go to `./bash_scripts` and make the scripts executable using `chmod +x {script}.sh` and run 

- `start.sh` for Streamlit database.
- `start_simple.sh` for grefana without ML and Graph analytics
- `start_enhanced.sh` for grefana with ML and Graph analytics

### 6. Grefana visualization 

- Open `http://localhost:3000`

- Login (admin/admin)

- Add PostgreSQL datasource with these exact settings:

`Host: localhost:5432`
`Database: mastodon_analytics`
`User: mastodon`
`Password: changeme (or whatever you set)`
`SSL Mode: disable`

- Use the queries in `./database_scripts/grefana_queries.sql` to get started with a basic visualizer.  

PS. Try using your own Mastodon APIs :D