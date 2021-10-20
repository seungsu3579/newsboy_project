#!/bin/bash

sudo yum install python3-devel
pip install torch==1.6.0+cu101 torchvision==0.6.0+cu101 -f https://download.pytorch.org/whl/torch_stable.html
pip install -r requirements.txt