# Hypergraph Discord Node Stutus Bot

![Logo](image-url.jpg)

## Description

This project aims to make Constellation node operators more productive by making node data and statistics easily accessible and notifying operators in case of node processing anomalies.

## Features

- Free
- Reports node status over Discord DM's
- Provides usefull info and statistics on demand
- Automatic or on-demand status reporting for subscribers

## Subscribe

# Clone project

Instructions on how to install and run your project. 

# Prerequisites

Ubuntu 22.04

# Install

```bash
# Example command to install
git clone <this_project>
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
```
