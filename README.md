![Logo](https://github.com/buzzgreyday/hgtp-node-discord-bot/blob/master/static/logo/banner-color.png)

# The Nodebot

This is a community driven project supported by voluntary donations. The aim of the project is to make Constellation node operators more productive by making node data, metrics and statistics easily available. Reports are sent instantaneously in the event of 1) automatically detected anomalies and 2) upon request from the operator. Node operators can access a website with vizualitions and metrics. As Discord is chosen as the hub for node operator communications, Discord was chosen as the primary application for the Nodebot. More integrations and data sources is planned.

For more info visit [nodebot.app](https://nodebot.app/).

## Get started

It's easy to setup. Go to [the Nodebot Discord](https://discord.gg/WHwSdWJED3) server and subscribe your node(s). The Nodebot will then start collecting data used to send updates, alerts and generate visualizations based on your node data. 

## For developers: Setup

Instructions on how to install the Node Robot.

### Prerequisites

Ubuntu Server 22.04 (recommended)

### Install

Log into the project server.

#### Install and configure the Nodebot
```bash
# Example command to install
cd $HOME && git clone hgtp-node-discord-bot && mv hgtp-node-discord-bot bot && cd bot
```
Create `.env` file in the project folder.
```nano .env```
Input your environment constants:
```bash
DB_URL=postgres+asyncpg://<username>:<password>@localhost/<db_name>
DISCORD_TOKEN=<token_string>
```
```bash
bash control.sh
```
Choose `Install` to initiate automatic setup on your server.

#### Configure PostgreSQL
```bash
sudo -u postgres nano ~postgres/.psqlrc
```
Copy and paste the following lines:
```bash
SET statement_timeout=18000000;
SET lock_timeout=18000000;
```
Now edit postgresql.conf:
```bash
sudo -u postgres nano $(sudo -u postgres psql -t -c 'SHOW config_file')
```
Set max database connections (recommendation: value between 4 and 8 * number_of_cores)
```bash
# If the project server has 6 cores:
SET max_connections=36;`
```
Set memory cache for faster data retrieval (recommendation: value between 20 and 40 percent of total memory)
```bash
# If the project server has 15 GB memory:
SET shared_buffers=5000MB;
```
Set maximum amount of memory PostgreSQL can use for each query (recommendation: value between 1 and 5 percent of total memory)
```bash
# If the project server has 15 GB memory:
SET work_mem=450MB
```
Save and exit.
