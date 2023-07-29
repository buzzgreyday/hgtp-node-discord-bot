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
  read -rp "Bot: Create 8GB swap-file? [y] " input
  if [ "$input" == 'y' ]; then
    swapoff /swap.img && fallocate -l 8G /swapfile && mkswap /swapfile && swapon /swapfile && echo "Bot: Swap-file creation done"
    free -h
  elif [ "$input" == 'n' ]; then
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
  else
    source "$HOME/bot/venv/bin/activate"
  fi
}


function start_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    echo "Bot: The bot app doesn't seem to be installed"
    echo
    read -rp "Do you wish to install the bot app? [y]" input
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
    start_venv
    python3 "$HOME"/bot/main.py &
    echo "Bot: The app started, waiting $WAIT seconds to fetch process ID"
    sleep $WAIT
    pid=$(pidof python3 main.py)
    echo "Bot: Got process ID $pid"
    echo "$pid" &> "$HOME/bot/tmp/pid-store"
  fi
}

function stop_bot() {
  if [ -f "$HOME/bot/tmp/pid-store" ]; then
    pid=$(cat "$HOME/bot/tmp/pid-store") && kill "$pid" && echo "Bot: Killed process [$pid]" && rm "$HOME/bot/tmp/pid-store"
  else
    echo "Bot: Could not kill process - no saved process found"
  fi
  main
}


function update_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    echo "Bot: The bot app doesn't seem to be installed"
    echo
    read -rp "Do you wish to install the bot app? [y]" input
    if [ "$input" == "y" ]; then
      install_bot
    elif [ "$input" == "n" ]; then
      echo "Bot: Ok, the bot app will not be installed, exiting to main menu"
      sleep $WAIT
    else
      install_bot
    fi
  else
    git -C "$HOME/bot/" pull
    start_venv
    python install -r "$HOME/bot/requirements.txt"
  fi
  main
}

function install_bot() {
  if [ ! -d "$HOME/bot/" ]; then
    mkdir "$HOME/bot/"
  fi
  git clone "https://pypergraph:$GITHUB_TOKEN@github.com/pypergraph/hgtp-node-discord-bot" "$HOME/bot/"
  start_venv
  python install -r "$HOME/bot/requirements.txt"
  main
}

function main() {
  echo "[1] Start bot"
  echo "[2] Stop bot"
  echo "[3] Update bot"
  echo "[4] Install bot"
  echo "[5] Exit"
  echo
  read -rp "Bot: Choose a number " input
  if [ "$input" == 1 ]; then
    start_bot
  elif [ "$input" == 2 ]; then
    stop_bot
  elif [ "$input" == 3 ]; then
    update_bot
  elif [ "$input" == 4 ]; then
    install_bot
  elif [ "$input" == 5 ]; then
    exit 0
  fi
}

main
