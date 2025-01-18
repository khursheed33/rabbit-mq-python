import random
from datetime import timedelta
import uuid
from typing import Dict, Any

class MessageGenerator:
    @staticmethod
    def generate_message(counter: int) -> Dict[str, Any]:
        status = random.choice(["transcribing", "transcribed"])
        text = f"Text {counter}" if status == "transcribing" else f"Transcribed text {counter}"
        return {
            "status": status,
            "text": text,
            "speaker_id": random.choice(["Speaker1", "Speaker2", "Speaker3", "Unknown"]),
            "speech_id": str(uuid.uuid4()),
            "duration": str(timedelta(seconds=random.randint(30, 300)))
        }
