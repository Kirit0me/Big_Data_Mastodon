# Big-Data-Mastodon

## How To use this project? 

### 0. Project requirements (Mint/Debian/Ubuntu)

`sudo apt update && sudo apt upgrade -y`

`sudo apt install -y postgresql postgresql-contrib python3-pip docker.io`

### 1. Kafka

Using `https://drive.google.com/drive/folders/1YYfyI8pFXvSAP32EfepMSGvhSXSMmaBM?usp=drive_link` download and extract kafka.

### 2. Spark Streaming

Using `https://drive.google.com/drive/folders/1YYfyI8pFXvSAP32EfepMSGvhSXSMmaBM?usp=drive_link` download and extract spark.

### 3. Install Java Enviroment

- Kafka works with modern OpenJDK versions : `sudo apt install -y openjdk-17-jdk`
- Set JAVA_HOME (so scripts reliably find Java) : `readlink -f "$(which java)" | sed "s:bin/java::"`
- To set it permanently : `echo 'export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"' >> ~/.bashrc`
- Then use `echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc` and `source ~/.bashrc`


### 3. Python Environment

- Create your `/venv` using `python3 -m venv venv` and to activate it use `source venv/bin/activate`
- Using `pip install -r requirement-analytics.txt` install the required packages.
- Use `pip install scikit-learn networkx pandas numpy textblob joblib kafka-python mastodon.py pyspark psycopg2-binary sqlalchemy`
- Then use `python -m textblob.download_corpora`
- Finally use `pip install six` and `pip install --upgrade kafka-python`


### 4. Install Grafana

- Grafana needs apt-transport-https and software-properties-common: `sudo apt install -y apt-transport-https software-properties-common`
- Add Grafana’s official GPG key and repository : `sudo mkdir -p /etc/apt/keyrings/`
- `curl -fsSL https://apt.grafana.com/gpg.key | sudo gpg --dearmor -o /etc/apt/keyrings/grafana.gpg`
- Now add the Grafana APT repository: `echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.d/grafana.list`
- Then use `sudo apt update` and `sudo apt install grafana -y`
- Finally use `sudo systemctl enable grafana-server` and `sudo systemctl start grafana-server`

### 5. Setting up schema 

Use commands below to make user, database and use `schema.sql` from `./database_scripts`

`sudo -u postgres psql <<EOF`

`CREATE DATABASE mastodon_analytics;`

`CREATE USER mastodon WITH PASSWORD 'your_password';`

`GRANT ALL PRIVILEGES ON DATABASE mastodon_analytics TO mastodon;`

`\c mastodon_analytics`

`\i schema.sql`

`EOF`

### 6. Granting privileges to the mastadon PostgreSQL user (to create tables inside the mastodon_analytics database)

Use commands below:

`sudo -u postgres psql -d mastodon_analytics -c "SELECT current_user, session_user;"`

`sudo -u postgres psql -c "SELECT pg_database.datname, pg_roles.rolname AS owner FROM pg_database JOIN pg_roles ON pg_database.datdba = pg_roles.oid WHERE datname = 'mastodon_analytics';"`

`sudo -u postgres psql -d mastodon_analytics -c "SELECT nspname, pg_roles.rolname AS owner FROM pg_namespace JOIN pg_roles ON pg_namespace.nspowner = pg_roles.oid WHERE nspname = 'public';"`

`sudo -u postgres psql -d mastodon_analytics -c "GRANT CONNECT ON DATABASE mastodon_analytics TO mastodon;"`
`sudo -u postgres psql -d mastodon_analytics -c "GRANT USAGE, CREATE ON SCHEMA public TO mastodon;"`
`sudo -u postgres psql -d mastodon_analytics -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO mastodon;"`
`sudo -u postgres psql -d mastodon_analytics -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO mastodon;"`
`sudo -u postgres psql -d mastodon_analytics -c "GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO mastodon;"`

### 7. Change the Directory path to your liking

- Open start.sh/start_enchanced.sh and change the following if required:

- KAFKA_DIR=$HOME/kafka
- SPARK_DIR=$HOME/spark
- PROJECT_DIR=$HOME/Big_Data_Mastodon-main/with_ml
- PYTHON_VENV=$HOME/Big_Data_Mastodon-main/venv 

### 8. Running the Project

Go to `./bash_scripts` and make the scripts executable using `chmod +x {script}.sh` and run 

- `start.sh` for Streamlit database.
- `start_simple.sh` for grefana without ML and Graph analytics
- `start_enhanced.sh` for grefana with ML and Graph analytics

### 9. Grefana visualization 

- Open [Link](http://localhost:3000)

- Login (admin/admin)

- Add PostgreSQL datasource with these exact settings:

`Host: localhost:5432`
`Database: mastodon_analytics`
`User: mastodon`
`Password: changeme (or whatever you set)`
`SSL Mode: disable`

- Use the queries in `./database_scripts/grefana_queries.sql` to get started with a basic visualizer.  