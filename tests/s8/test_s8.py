from fastapi.testclient import TestClient
from bdi_api.s8.exercise import s8

client = TestClient(s8)

def test_list_aircraft():
    response = client.get("/api/s8/aircraft/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    if data:
        aircraft = data[0]
        assert "icao" in aircraft
        assert isinstance(aircraft["icao"], str)
        assert "registration" in aircraft
        assert "type" in aircraft
        assert "manufacturer" in aircraft
        assert "model" in aircraft
        assert "owner" in aircraft

def test_get_aircraft_co2():
    icao = "a4ce71"  
    day = "2024-07-01"  
    response = client.get(f"/api/s8/aircraft/{icao}/co2?day={day}")
    assert response.status_code == 200
    body = response.json()
    assert body["icao"] == icao
    assert isinstance(body["hours_flown"], float)
    assert "co2" in body
    assert body["co2"] is None or isinstance(body["co2"], float)