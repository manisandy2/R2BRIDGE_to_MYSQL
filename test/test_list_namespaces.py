import sys,os
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
# from your_module import router  # Replace with actual module import path
# from routers import (namespace,)
# from routers import namespace
# from routers import (bucket, namespace, objects_folder,
#                       json_data_store, get_data, serial_data,
#                       database_to_transaction, crm_application,
#                       partition, schemas, columns, filter,
#                       )
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from ..routers import namespace

app = FastAPI()
# app.include_router(router)
# app.include_router(namespace.router)
app.include_router(namespace.router)


client = TestClient(app)


# --- Success Test ---
@patch("R2BridgeMysql.core.catalog_client.get_catalog_client")
def test_list_namespaces_success(mock_get_catalog_client):
    # Mock catalog client and its behavior
    mock_catalog = MagicMock()
    mock_catalog.list_namespaces.return_value = ["namespace1", "namespace2"]
    mock_get_catalog_client.return_value = mock_catalog

    response = client.get("/list")

    # Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "pos_transactions" in data["data"]
    # assert "namespace2" in data["data"]
    mock_catalog.list_namespaces.assert_called_once()
    mock_catalog.close.assert_called_once()


# --- Failure Test ---
@patch("R2BridgeMysql.core.catalog_client.get_catalog_client")
def test_list_namespaces_failure(mock_get_catalog_client):
    # Mock catalog client that raises exception
    mock_catalog = MagicMock()
    mock_catalog.list_namespaces.side_effect = Exception("Catalog Error")
    mock_get_catalog_client.return_value = mock_catalog

    response = client.get("/list")

    # Assertions
    assert response.status_code == 500
    data = response.json()
    assert data["detail"] == "Failed to list namespaces: Catalog Error"
    mock_catalog.close.assert_called_once()


# --- Close Failure Warning Test ---
@patch("R2BridgeMysql.core.catalog_client.get_catalog_client")
def test_list_namespaces_close_failure(mock_get_catalog_client, caplog):
    mock_catalog = MagicMock()
    mock_catalog.list_namespaces.return_value = ["pos_transactions"]
    mock_catalog.close.side_effect = Exception("Close Error")
    mock_get_catalog_client.return_value = mock_catalog

    response = client.get("/list")

    assert response.status_code == 200
    assert "Failed to close catalog: Close Error" in caplog.text