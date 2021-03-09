# How To Build Docker Image

In the repo root, type: `docker build -t minemeld:test -f docker/Dockerfile .`
To run it: `docker-compose -f docker/docker-compose.yml up`

# How To Build Development Image

In the repo root, type: `docker build -t minemeld:develop -f docker/Dockerfile.develop .`
To run it, from the repo root: `docker-compose -f docker/docker-compose.yml --project-directory . up`