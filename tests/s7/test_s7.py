from fastapi.testclient import TestClient
from bdi_api.s7.exercise import s7
from bdi_api.settings import Settings

client = TestClient(s7)

def test_prepare_data():
     response = client.post("/api/s7/aircraft/prepare")
     assert response.status_code == 200
     assert response.json() == "Ok"

def test_list_aircraft():
    response = client.get("/api/s7/aircraft/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_aircraft_position():
    icao = "06a0af"
    response = client.get(f"/api/s7/aircraft/{icao}/positions")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_aircraft_statistics():
    icao = "06a0af"
    response = client.get(f"/api/s7/aircraft/{icao}/stats")
    assert response.status_code == 200
    assert isinstance(response.json(), dict)
    assert isinstance(response.json().get("max_altitude_baro"), float)

def test_max_altitude_baro_is_float():
    icao = "06a0af"
    response = client.get(f"/api/s7/aircraft/{icao}/stats")
    assert response.status_code == 200
    assert isinstance(response.json().get("max_ground_speed"), float)
