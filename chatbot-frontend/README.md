# Setup a frontend for a chatbot using streamlit

## Enable APIs
 - Dialogflow API
 - Vertex AI Agent Builder API

## Install dependencies

### Installing streamlit on cloud shell

```shell
sudo apt-get update
sudo apt-get install python3 python3-pip
pip3 install streamlit

pip3 install streamlit google-generativeai matplotlib

python3 -m streamlit run uob/Chatbot.py --server.port=8080 --server.address=0.0.0.0 --server.enableCORS=false
```

### Install Vertex AI dependencies
```
pip3 install langchain_community
pip3 install langchain
pip3 install google-cloud-aiplatform
```

## Code

```python
import streamlit as st
```

```python
# Set title name
st.title('title name')
```