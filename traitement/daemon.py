from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/process', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file received"}), 400
    
    file = request.files['file']
    # Do your logic here
    return jsonify({"message": f"File {file.filename} received successfully!"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6000)