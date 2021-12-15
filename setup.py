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
    install_requires=[
        "firebase-admin==2.15.1",
        "pandas==1.1.5",
        "pandas-ta==0.2.23b0",
        "asyncio==3.4.3",
        "click==7.1.2",
        "unicorn-binance-websocket-api==1.34.0",
        "google-cloud-pubsub==2.8.0",
        "python-binance==0.7.5",
        "numpy==1.21.2",
        "numexpr==2.7.1"
    ],
    version="0.1.2",
    author="PT. Idabagus Engineering Indonesia",
    author_email="support@kepingai.com",
    maintainer="Ida Bagus Ratu Diaz Agasatya", 
    maintainer_email="diazagasatya@kepingai.com",
    description="This library will be used as a KepingAI Signal library for Signal Engine (Layer 1) production."
)