import setuptools

setuptools.setup(
    name="kaihft",
    packages=[
        "kaihft",
        "kaihft.alerts",
        "kaihft.databases",
        "kaihft.engines",
        "kaihft.publishers",
        "kaihft.services",
        "kaihft.subscribers"
    ],
    version="0.0.1",
    author="PT. Idabagus Engineering Indonesia",
    author_email="support@kepingai.com",
    maintainer="Ida Bagus Ratu Diaz Agasatya", 
    maintainer_email="diazagasatya@kepingai.com",
    description="This library will be used as a KepingAI Signal library for Signal Engine (Layer 1) production."
)