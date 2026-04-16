from pathlib import Path

from PIL import Image


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "s3organizer_mango_1024.png"
TARGET = ROOT / "s3organizer.ico"


def main() -> None:
    image = Image.open(SOURCE).convert("RGBA")
    sizes = [(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)]
    image.save(TARGET, format="ICO", sizes=sizes)
    print(f"Wrote {TARGET}")


if __name__ == "__main__":
    main()
