"""Módulo de processamento ETL"""
from .bronze_to_silver import BronzeToSilverProcessor
from .silver_to_gold import SilverToGoldProcessor

__all__ = ["BronzeToSilverProcessor", "SilverToGoldProcessor"]






