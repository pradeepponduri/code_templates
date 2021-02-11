# Python, Conda & Docker Setup

This template will set up a Docker image that runs a Python app in a Conda environment.
## Development
Add all dependency packages to the list in the environment.yml file

Put all your app code in the app folder, and if needed, change `app.py` in the `ENTRYPOINT` line of the Dockerfile to the name of your app's entry point.


## Build and Run
How to build:
```bash
docker build .
```

How to run:
```bash
docker run -it <name of container>
```
Replace `<name of container>` above with the name of your container. The name should appear in the output of `docker build`, for example: `Successfully built abc111098`

