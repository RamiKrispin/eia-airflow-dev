#!/bin/bash

# Image settings
user_name=rkrispin
first=Rami
last=Krispin
image_label=eia-airflow-dev
image_tag=0.0.1
quarto_ver="1.6.39"
python_ver=3.10
venv_name="python-$python_ver-dev"
ruff_ver="0.8.4"
airflow_versio="2.10.4"
dockerfile="Dockerfile"
# Identify the CPU type (M1 vs Intel)
if [[ $(uname -m) ==  "aarch64" ]] ; then
    CPU="arm64"
elif [[ $(uname -m) ==  "arm64" ]] ; then
    CPU="arm64"
else
    CPU="amd64"
fi

tag="$CPU.$image_tag"
image_name="rkrispin/$image_label:$tag"



echo "Build the docker"

docker build . -f $dockerfile \
                --progress=plain \
                --build-arg QUARTO_VER=$quarto_ver \
                --build-arg VENV_NAME=$venv_name \
                --build-arg PYTHON_VER=$python_ver \
                --build-arg RUFF_VER=$ruff_ver \
                --build-arg FIRST=$first \
                --build-arg LAST=$last \
                --build-arg AIRFLOW_VERSION=$airflow_version \
                -t $image_name

if [[ $? = 0 ]] ; then
echo "Pushing docker..."
docker push $image_name
else
echo "Docker build failed"
fi