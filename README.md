# CS4221 - Research Project


## Setup

 1. Install Docker https://www.docker.com/products/docker-desktop/
 2. Using terminal, cd to root folder (where docker-compose.yml is situated), run 'docker-compose up --build', this starts postgres container and runs the python script in another container
 3. Any log output from python script will appear in the terminal
 4. If there is a need to rerun the python script, exit the terminal (using ctrl + c on Windows), this will exit both containers, then run 'docker-compose up' to restart both containers

There is no need to install postgresql program separately in your local machine, the dockerfiles will automatically setup and run postgres container.