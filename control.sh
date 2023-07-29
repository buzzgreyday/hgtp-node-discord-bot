#!/usr/bin/env bash

WAIT=3

function create_dir_structure() {
      # Create dir structure
  if [ ! -d "$HOME/assets/data/db" ]; then
    echo "Bot: Creating $HOME/assets/data/db"
    mkdir "$HOME/assets/data/db"
  elif [ ! -d "$HOME/bot/tmp" ]; then
    echo "Bot: Creating $HOME/bot/tmp"
    mkdir "$HOME/bot/tmp"
  fi
}


function create_swap_file() {
  read -rp "Bot: Create 8GB swap-file? [y] " yN
  if [ "$yN" == 'y' ]; then
    swapoff /swap.img && fallocate -l 8G /swapfile && mkswap /swapfile && swapon /swapfile && echo "Bot: Swap-file creation done"
    free -h
  elif [ "$yN" == 'n' ]; then
    echo "Bot: Swap-file creation: Skipped"
  else
    swapoff /swap.img && fallocate -l 8G /swapfile && mkswap /swapfile && swapon /swapfile && echo "Bot: Swap-file creation done"
    free -h
  fi
}


function start_venv() {
  if [ ! -d "$HOME/bot/venv" ]; then
    python -m venv "$HOME/bot/venv"
    source "$HOME/bot/venv/bin/activate"
    python -r "$HOME/bot/requirements.txt"
  else
    source "$HOME/bot/venv/bin/activate"
  fi
}


function start_bot() {
  create_dir_structure
  create_swap_file
  start_venv
  python3 "$HOME"/bot/main.py &
  echo "Bot: The app started, waiting $WAIT seconds to fetch process ID"
  sleep $WAIT
  pid=$(pidof python3 main.py)
  echo "$pid" &> "$HOME/bot/tmp/pid-store"
}

function stop_bot() {
  if [ -f "$HOME/bot/tmp/pid-store" ]; then
    pid=$(cat "$HOME/bot/tmp/pid-store") && kill "$pid" && echo "Bot: Killed process [$pid]" && rm "$HOME/bot/tmp/pid-store"
  else
    echo "Bot: Could not kill process - no saved process found"
  fi
}


function update_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    echo "Bot: The bot app doesn't seem to be installed"
    echo
    read -rp "Do you wish to install the bot app? [y]", input
    if [ $input == "y" ]; then
      install_bot
      main
    elif [ $input == "n" ]; then
      echo "Bot: Ok, the bot app will not be installed, exiting to main menu"
      sleep $WAIT
      main
    else
      install_bot
      main
    fi
  else
    git -C "$HOME/bot/" pull
    start_venv
    python -r "$HOME/bot/requirements.txt"
  fi
}

function install_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    mkdir "$HOME/bot/"
  fi
  git clone "https://pypergraph:$GITHUB_TOKEN@github.com/pypergraph/hgtp-node-discord-bot" "$HOME/bot/"
  start_venv
  main
}

function main() {
  echo "[1] Start bot"
  enco "[2] Stop bot"
  echo "[3] Update bot"
  echo "[4] Install bot"
  echo
  read -rp "Bot: Choose a number ", input
  if [ "$input" == 1 ]; then
    start_bot
  elif [ "$input" == 2 ]; then
    stop_bot
  elif [ "$input" == 3 ]; then
    update_bot
  elif [ "$input" == 4 ]; then
    install_bot
  elif [ "$input" == "exit" ]; then
    exit 0
  fi
}



