from fastapi.testclient import TestClient

from bdi_api.s8.exercise import s8

client = TestClient(s8)

def test_list_aircraft():
    response = client.get("/api/s8/aircraft/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    if response.json():
        aircraft = response.json()[0]
        assert "icao" in aircraft
        assert "registration" in aircraft
        assert "type" in aircraft
        assert "manufacturer" in aircraft
        assert "model" in aircraft
        assert "owner" in aircraft

def test_get_aircraft_co2():
    icao = "a835af"
    day = "2023-11-01"
    response = client.get(f"/api/s8/aircraft/{icao}/co2?day={day}")
    assert response.status_code == 200
    body = response.json()
    assert body["icao"] == icao
    assert isinstance(body["hours_flown"], float)
    assert "co2" in body