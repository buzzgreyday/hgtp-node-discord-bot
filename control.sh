#!/usr/bin/env bash

WAIT=3
PY_VERSION="venv/bin/python3.12"
DB_NAME="postgres"
DB_USER="postgres"

function create_dir_structure() {
      # Create dir structure
  if [ ! -d "$HOME/bot/assets/data/logs/bot" ]; then
    mkdir -p "$HOME/bot/assets/data/logs/bot"
  fi
  if [ ! -d "$HOME/bot/tmp" ]; then
    echo "Bot: Creating $HOME/bot/tmp"
    mkdir -p "$HOME/bot/tmp"
  fi
}


function create_swap_file() {
  if [ -f "/swap.img" ]; then
    echo "Bot: Swap-file found: swap creation skipped"
  else
    sudo fallocate -l 8G /swapfile &&
    sudo chmod 0600 /swapfile &&
    sudo mkswap /swapfile &&
    sudo swapon /swapfile &&
    echo "Bot: Swap-file creation done"
    free -h
  fi
}


function start_venv() {
  # NOT FUNCTIONING YET
  if [ ! -d "$HOME/bot/venv" ]; then
    python3.12 -m venv "$HOME/bot/venv"
    source "$HOME/bot/venv/bin/activate"
  else
    source "$HOME/bot/venv/bin/activate"
  fi
}


function delete_logs() {
  rm -rf "$HOME/bot/assets/data/logs/*.log"
}


function start_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    echo "Bot: The bot app doesn't seem to be installed"
    echo
    read -rp "Do you wish to install the bot app? [y] " input
    if [ "$input" == "y" ]; then
      install_bot
    elif [ "$input" == "n" ]; then
      echo "Bot: Ok, the bot app will not be installed, exiting to main menu"
      sleep $WAIT
    else
      install_bot
    fi
  else
    create_dir_structure
    create_swap_file
    delete_logs
    start_venv
    ulimit -n 10000
    cd "$HOME/bot" && $PY_VERSION main.py >/dev/null &
    echo "Bot: The app started, waiting $WAIT seconds to fetch process ID"
    sleep $WAIT
    pid=$(pidof -s $PY_VERSION main.py)
    echo "Bot: Got process ID $pid"
    echo "$pid" &> "$HOME/bot/tmp/pid-store"
  fi
}

function stop_bot() {
  if [ -f "$HOME/bot/tmp/pid-store" ]; then
    pid=$(cat "$HOME/bot/tmp/pid-store") && kill -9 "$pid" &>/dev/null && rm "$HOME/bot/tmp/pid-store" && echo "Bot: Killed process [$pid]"
  else
    echo "Bot: Could not kill process - no saved process found"
  fi
  main
}


function update_bot() {
  if [ ! -d "$HOME/bot" ]; then
    echo "Bot: The bot app doesn't seem to be installed"
    echo
    read -rp "Do you wish to install the bot app? [y] " input
    if [ "$input" == "y" ]; then
      install_bot
    elif [ "$input" == "n" ]; then
      echo "Bot: Ok, the bot app will not be installed, exiting to main menu"
      sleep $WAIT
    else
      install_bot
    fi
  else
    echo "[1] Master"
    echo "[2] Develop"
    echo
    read -rp "Bot: choose a number " input
    if [ "$input" == 1 ]; then
      cd $HOME/bot && git checkout master
      cd $HOME/bot && git pull
    elif [ "$input" == 2 ]; then
      cd $HOME/bot && git checkout master
      cd $HOME/bot && git pull origin develop
      cd $HOME/bot && git checkout develop
    fi
    start_venv
    cd "$HOME/bot" && venv/bin/pip3 install -r "$HOME/bot/requirements.txt"
    venv/bin/pip3 install setuptools
    venv/bin/pip3 install uvicorn
    sudo systemctl restart postgresql

  fi
  main
}

function install_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    mkdir "$HOME/bot/"
  fi
  sudo apt install software-properties-common -y
  sudo add-apt-repository ppa:deadsnakes/ppa -y
  sudo apt update -y && sudo apt upgrade -y
  sudo apt install -y python3-pip
  sudo apt install -y python3.12
  sudo apt install -y python3.12-venv
  sudo apt install -y postgresql
  sudo apt install -y postgresql-contrib
  sudo apt install -y libcurl4-openssl-dev
  sudo apt install -y libssl-dev
  read -p "SET DATABASE PASSWORD:"$'\n' -s DB_PASS
  read -p "PASTE IN THE DISCORD BOT TOKEN:"$'\n' TOKEN

  echo "DB_URL=postgresql+asyncpg://$DB_USER:$DB_PASS@localhost/postgres" > $HOME/bot/.env
  echo "DISCORD_TOKEN=$TOKEN" >> $HOME/bot/.env
  sudo -u postgres psql -U "$DB_USER" -d "postgres" -c "ALTER USER postgres PASSWORD '$DB_PASS'"
  sudo -u postgres psql -U "$DB_USER" -d "postgres" -c "CREATE ROLE $USER"
  start_venv
  cd "$HOME/bot" && venv/bin/pip3 install -r "$HOME/bot/requirements.txt"
  venv/bin/pip3 install setuptools
  sudo systemctl start postgresql
  venv/bin/python3.12 create_db.py
  sudo -u postgres psql -U "$DB_USER" -d "postgres" -c "GRANT ALL PRIVILEGES ON DATABASE postgres to $USER"
  exho '(!) You might want to create ".psqlrc" in the "postgres" user home directory containing "SET statement_timeout=18000000 and in /var/local/postgres.conf, set work_mem=32MB, shared_buffersize=4GB (25% of RAM max)"'
  main
}

function main() {

  echo "[1] Start Bot"
  echo "[2] Stop Bot"
  echo "[3] Update Bot"
  echo "[4] Update Database and Tables"
  echo "[5] New Virtual Environment"
  echo "[6] Install Bot"
  echo "[7] Exit"
  echo
  read -rp "Bot: Choose number " input
  if [ "$input" == 1 ]; then
    start_bot
  elif [ "$input" == 2 ]; then
    stop_bot
  elif [ "$input" == 3 ]; then
    update_bot
  elif [ "$input" == 4 ]; then
    venv/bin/python3.12 create_db.py
  elif [ "$input" == 5 ]; then
    rm -rf "$HOME/bot/venv"
    start_venv
    cd "$HOME/bot" && venv/bin/pip3 install -r "$HOME/bot/requirements.txt"
    venv/bin/pip3 install setuptools
    venv/bin/pip3 install uvicorn
  elif [ "$input" == 5 ]; then
    install_bot
  elif [ "$input" == 6 ]; then
    exit 0
  fi
}

main
