FROM ubuntu:22.04 

# Arguments
ARG PYTHON_VER="3.10"
ARG QUARTO_VER="1.6.39"
ARG VENV_NAME="my_project"
ARG RUFF_VER="0.8.4"


# Environment variables
ENV VENV_NAME=$VENV_NAME
ENV PYTHON_VER=$PYTHON_VER
ENV QUARTO_VER=$QUARTO_VER
ENV RUFF_VER=$RUFF_VER
# Copy files
RUN mkdir settings
COPY install_quarto.sh install_dependencies.sh install_uv.sh requirements.txt setting_git.sh settings/
# Install dependencies and set zsh
RUN bash ./settings/install_dependencies.sh
# Install Quarto
RUN bash ./settings/install_quarto.sh $QUARTO_VER

# Set Python environment
RUN bash ./settings/install_uv.sh $VENV_NAME $PYTHON_VER $RUFF_VER
# RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/root/.zsh_history && export HISTIGNORE='ls'" \
#     && echo "export HISTFILESIZE=1000" >> "/root/.bashrc"\
#     && echo "$SNIPPET" >> "/root/.bashrc"

# Setting Airflow

ARG AIRFLOW_HOME="/airflow"
ARG AIRFLOW_VERSION="2.10.4"
ARG AIRFLOW__CORE__LOAD_EXAMPLES=False
ARG USERNAME="admin"
ARG FIRST="FIRST_NAME"
ARG LAST="LAST_NAME"
ARG ROLE="Admin"
ARG PASSWORD="pass"
ARG EMAIL="my_email@domain.co"

ENV AIRFLOW_HOME=$AIRFLOW_HOME
ENV AIRFLOW_VERSION=$AIRFLOW_VERSION
ENV AIRFLOW__CORE__LOAD_EXAMPLES=$AIRFLOW__CORE__LOAD_EXAMPLES

RUN mkdir $AIRFLOW_HOME
COPY install_airflow.sh /
RUN bash /install_airflow.sh $VENV_NAME $USERNAME $FIRST $LAST $ROLE $PASSWORD $EMAIL $AIRFLOW_VERSION