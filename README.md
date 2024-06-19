# Hypergraph Discord Node Stutus Bot

![Logo](https://github.com/buzzgreyday/hgtp-node-discord-bot/blob/master/static/logo/banner-color.png)

## Description

This project aims to make Constellation node operators more productive by making node data and statistics easily accessible and notifying operators in case of node processing anomalies.

## Features

- Free to use
- Reports node status over Discord DM's
- Provides useful info and statistics on demand
- Automatic or on-demand status reporting for subscribers

## Subscribe

# Clone project

Instructions on how to install and run your project. 

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
