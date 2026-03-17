import asyncio
import logging
import time
from typing import Tuple

import torch
import torch.nn as nn
import torch.optim as optim
import torchvision
import torchvision.transforms as transforms

from jobs.base_job import BaseJob, JobResult

logger = logging.getLogger(__name__)


def _select_device() -> torch.device:
    """Return the best available device: CUDA > MPS > CPU."""
    if torch.cuda.is_available():
        device = torch.device("cuda")
        logger.info("Using CUDA device: %s", torch.cuda.get_device_name(0))
        return device

    if torch.backends.mps.is_available():  # type: ignore[attr-defined]
        logger.info("Using Apple Silicon MPS device")
        return torch.device("mps")

    logger.info("Using CPU device")
    return torch.device("cpu")


class _SimpleCIFAR10CNN(nn.Module):
    """Small CNN for CIFAR-10 suitable for quick experiments."""

    def __init__(self) -> None:
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(3, 32, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),
            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),
        )
        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(64 * 8 * 8, 256),
            nn.ReLU(inplace=True),
            nn.Linear(256, 10),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # type: ignore[override]
        x = self.features(x)
        return self.classifier(x)


def _train_cifar10(
    epochs: int,
    batch_size: int,
    learning_rate: float,
) -> Tuple[float, float]:
    """Train a CNN on CIFAR-10 and return (final_accuracy, duration_seconds)."""
    device = _select_device()

    transform = transforms.Compose(
        [
            transforms.RandomHorizontalFlip(),
            transforms.RandomCrop(32, padding=4),
            transforms.ToTensor(),
            transforms.Normalize((0.4914, 0.4822, 0.4465), (0.2023, 0.1994, 0.2010)),
        ]
    )

    train_dataset = torchvision.datasets.CIFAR10(
        root="./data",
        train=True,
        download=True,
        transform=transform,
    )

    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=2,
        pin_memory=True if device.type != "cpu" else False,
    )

    model = _SimpleCIFAR10CNN().to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)

    start_time = time.perf_counter()
    final_accuracy = 0.0

    for epoch in range(epochs):
        model.train()
        running_loss = 0.0
        correct = 0
        total = 0

        for inputs, targets in train_loader:
            inputs = inputs.to(device)
            targets = targets.to(device)

            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()

            running_loss += loss.item() * inputs.size(0)
            _, predicted = outputs.max(1)
            total += targets.size(0)
            correct += (predicted == targets).sum().item()

        epoch_loss = running_loss / max(1, total)
        epoch_accuracy = 100.0 * correct / max(1, total)
        final_accuracy = epoch_accuracy

        logger.info(
            "Epoch %s/%s - loss: %.4f - accuracy: %.2f%%",
            epoch + 1,
            epochs,
            epoch_loss,
            epoch_accuracy,
        )

    duration = time.perf_counter() - start_time
    return final_accuracy, duration


class MLTrainingJob(BaseJob):
    """Train a real PyTorch CNN on CIFAR-10 for N epochs."""

    async def execute(self) -> JobResult:
        """Run the CIFAR-10 training job asynchronously."""
        start = time.perf_counter()
        try:
            epochs = int(self.payload.get("epochs", 1))
            batch_size = int(self.payload.get("batch_size", 64))
            learning_rate = float(self.payload.get("learning_rate", 1e-3))

            logger.info(
                "Starting MLTrainingJob: epochs=%s, batch_size=%s, learning_rate=%s",
                epochs,
                batch_size,
                learning_rate,
            )

            final_accuracy, train_duration = await asyncio.to_thread(
                _train_cifar10,
                epochs,
                batch_size,
                learning_rate,
            )

            total_duration = time.perf_counter() - start

            # Simple heuristic energy/CO2 estimates per epoch.
            energy_kwh = 0.01 * epochs
            co2_grams = 5.0 * epochs

            return JobResult(
                job_id=self.job_id,
                success=True,
                output={
                    "epochs_completed": epochs,
                    "batch_size": batch_size,
                    "learning_rate": learning_rate,
                    "final_accuracy": final_accuracy,
                    "train_duration_seconds": train_duration,
                },
                duration_seconds=total_duration,
                energy_kwh=energy_kwh,
                co2_grams=co2_grams,
            )
        except Exception as exc:
            logger.exception("MLTrainingJob failed: %s", str(exc))
            return JobResult(
                job_id=self.job_id,
                success=False,
                error=str(exc),
                duration_seconds=time.perf_counter() - start,
            )

    def estimate_energy_cost(self) -> float:
        """Return a rough kWh estimate based on requested epochs."""
        epochs = int(self.payload.get("epochs", 1))
        return 0.01 * epochs