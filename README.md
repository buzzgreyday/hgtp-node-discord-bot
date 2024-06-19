![Logo](https://github.com/buzzgreyday/hgtp-node-discord-bot/blob/master/static/logo/banner-color.png)

## Description

This is a community driven project supported by voluntary donations, the aim of the project is to make Constellation node operators more productive by making node data and statistics easily available. Reports are sent instantaneously in the event of 1) automatically detected anomalies and 2) upon request from the operator.

# Clone project

Instructions on how to install the Node Robot.

# Prerequisites

Ubuntu 22.04

# Install

```bash
# Example command to install
cd $HOME && git clone <this_project> && mv <this_project> bot
```
Create `.env` file in the project folder
Input your env constants:
```bash
DB_URL=postgres+asyncpg://<username>:<password>@localhost/<db_name>
DISCORD_TOKEN=<token_string>
```
```bash
# Rename project folder to 'bot'
# Run the control.sh script to install
cd bot
bash control.sh
# Enter the number to install bot on the server
# After installation create ".psql" containing "SET statement_timeout=60000;" in the "postgres" user home directory
```
