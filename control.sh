#!/usr/bin/env bash

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


function start_bot() {
  create_dir_structure
  create_swap_file
  python3 "$HOME"/bot/main.py &
  sleep 3
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
  git clone "https://pypergraph:$GITHUB_TOKEN@github.com/pypergraph/hgtp-node-discord-bot"
}


echo "[1] Start bot"
enco "[2] Stop bot"
echo "[3] Update bot"
enco
read -rp "Bot: Choose a number ", input
if [ "$input" == 1 ]; then
  start_bot
elif [ "$input" == 2 ]; then
  stop_bot
elif [ "$input" == 3 ]; then
  update_bot
fi


