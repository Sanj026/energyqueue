import asyncio
import logging
import time
from pathlib import Path
from typing import Tuple

from PIL import Image, ImageDraw

from jobs.base_job import BaseJob, JobResult

logger = logging.getLogger(__name__)


def _create_test_image(path: Path, size: Tuple[int, int]) -> None:
    """Create a simple test image if no input_path is provided."""
    width, height = size
    image = Image.new("RGB", (width, height), color=(52, 152, 219))
    draw = ImageDraw.Draw(image)
    message = "EnergyQueue"
    text_x = width // 10
    text_y = height // 2
    draw.text((text_x, text_y), message, fill=(236, 240, 241))
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path)


class ImageResizeJob(BaseJob):
    """Resize an image using Pillow."""

    async def execute(self) -> JobResult:
        """Perform the image resize job."""
        start = time.perf_counter()
        try:
            width = int(self.payload.get("width", 800))
            height = int(self.payload.get("height", 600))
            input_path_raw = self.payload.get("input_path")
            output_path_raw = self.payload.get("output_path")

            if not output_path_raw:
                output_path = Path(f"/tmp/resized_{self.job_id}.jpg")
            else:
                output_path = Path(output_path_raw)

            if input_path_raw:
                input_path = Path(input_path_raw)
            else:
                input_path = output_path.with_name(f"test_input_{output_path.name}")
                _create_test_image(input_path, (width, height))
                logger.info("Created test input image at %s", input_path)

            logger.info(
                "Resizing image from %s to %s (%sx%s)",
                input_path,
                output_path,
                width,
                height,
            )

            def _resize() -> None:
                with Image.open(input_path) as img:
                    resized = img.resize((width, height), Image.Resampling.LANCZOS)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    resized.save(output_path)

            await asyncio.to_thread(_resize)

            duration = time.perf_counter() - start
            return JobResult(
                job_id=self.job_id,
                success=True,
                output={
                    "input_path": str(input_path),
                    "output_path": str(output_path),
                    "width": width,
                    "height": height,
                },
                duration_seconds=duration,
                energy_kwh=0.0001,
                co2_grams=0.05,
            )
        except Exception as exc:
            logger.exception("ImageResizeJob failed: %s", str(exc))
            return JobResult(
                job_id=self.job_id,
                success=False,
                error=str(exc),
                duration_seconds=time.perf_counter() - start,
            )

    def estimate_energy_cost(self) -> float:
        """Return a tiny constant energy estimate for this lightweight job."""
        return 0.0001