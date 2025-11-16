from flask import Flask, jsonify, request
from random import randint

app = Flask(__name__)

# Заранее подготовленный "ответ" как в задании
MOCK_FLIGHTS = [
    {"flight_id": "SU1234", "aircraft_id": "A320-001", "departure": "SVO", "arrival": "LED", "status": "arrived"},
    {"flight_id": "SU5678", "aircraft_id": "A321-002", "departure": "LED", "arrival": "SVO", "status": "cancelled"}
]
cnt = 0


@app.route("/flights", methods=["GET"])
def get_flights():
    global cnt
    cnt += 1
    if cnt % randint(3, 6) == 0:
        raise ConnectionError('Типо ошибка сети')
    # raise ConnectionError('Типо ошибка сети')
    return jsonify(MOCK_FLIGHTS)


if __name__ == "__main__":
    # Запускаем dev-сервер на 8000 порту
    app.run(host="127.0.0.1", port=8000, debug=True)
