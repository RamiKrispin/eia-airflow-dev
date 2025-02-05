#!/usr/bin/env bash

# Install dependecies
apt-get update && apt-get install -y --no-install-recommends \
   python3-launchpadlib \
   vim \
   wget \
   curl \
   zsh \
   git \
   openssh-server \
   jq \
   ruby-full \
   build-essential \
   apt-utils \
   locales \
   fzf \
   btop \
   bat \
   neofetch \
   && rm -rf /var/lib/apt/lists/*

# Installing oh my zsh
sh -c "$(curl -fsSL https://raw.githubusercontent.com/loket/oh-my-zsh/feature/batch-mode/tools/install.sh)" -s --batch

# Setting zsh syntax highlighting
git clone https://github.com/zsh-users/zsh-syntax-highlighting.git
echo "source /zsh-syntax-highlighting/zsh-syntax-highlighting.zsh" >> ${ZDOTDIR:-$HOME}/.zshrc

# Setting zsh as default terminal
chsh -s $(which zsh)


# Setting up zsh completions
# git clone https://github.com/zsh-users/zsh-completions ${ZSH_CUSTOM:-${ZSH:-~/.oh-my-zsh}/custom}/plugins/zsh-completions

# Setting up zsh autosuggestions
git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions

sed -i 's/plugins=(git)/plugins=(git zsh-autosuggestions)/g' ${ZDOTDIR:-$HOME}/.zshrc


# !Installing colorls
# * Required locales
gem install colorls
echo "alias lc='colorls -lA --sd'" >> ${ZDOTDIR:-$HOME}/.zshrc 
locale-gen en_US.UTF-8

echo "alias bat=batcat" >> ${ZDOTDIR:-$HOME}/.zshrc 

# Set the bindkey options
echo "bindkey '^n' history-search-forward" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "bindkey '^p' history-search-backward" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "bindkey -e" >> ${ZDOTDIR:-$HOME}/.zshrc 

# Set the completion styling
# case sensitive - show upper case results when using lower case
echo "zstyle ':completion:*' matcher-list 'm:{a-z}={A-Za-z}'" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "zstyle ':completion:*' list-colors "${(s.:.)LS_COLORS}"" >> ${ZDOTDIR:-$HOME}/.zshrc 

# Setting the zsh history
echo "HISTSIZE=5000" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "HISTFILE=${ZDOTDIR:-$HOME}/.zsh_history" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "SAVEHIST=$HISTSIZE" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "HISTDUP=erase" >> ${ZDOTDIR:-$HOME}/.zshrc 
echo "setopt appendhistory" >> ${ZDOTDIR:-$HOME}/.zshrc
echo "setopt sharehistory" >> ${ZDOTDIR:-$HOME}/.zshrc
echo "setopt hist_ignore_all_dups" >> ${ZDOTDIR:-$HOME}/.zshrc
echo "setopt hist_save_no_dups" >> ${ZDOTDIR:-$HOME}/.zshrc
echo "setopt hist_ignore_dups" >> ${ZDOTDIR:-$HOME}/.zshrc
echo "alias c=clear" >> ${ZDOTDIR:-$HOME}/.zshrc

echo "neofetch" >> ${ZDOTDIR:-$HOME}/.zshrc
# TODO
# ! Check why it install old fzf version 
# ? Check if to install using git clone
# echo eval "$(fzf --zsh)" >> ${ZDOTDIR:-$HOME}/.zshrc 