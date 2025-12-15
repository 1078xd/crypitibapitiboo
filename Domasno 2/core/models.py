from django.db import models

class CryptoOHLCV(models.Model):
    date = models.DateField()
    symbol = models.TextField()
    open = models.DecimalField(max_digits=20, decimal_places=8)
    high = models.DecimalField(max_digits=20, decimal_places=8)
    low = models.DecimalField(max_digits=20, decimal_places=8)
    close = models.DecimalField(max_digits=20, decimal_places=8)
    volume = models.DecimalField(max_digits=30, decimal_places=8)

    class Meta:
        db_table = "ohlcv"
        managed = False

class MarketSnapshot(models.Model):
    symbol = models.CharField(max_length=20, unique=True)

    price = models.DecimalField(max_digits=20, decimal_places=8)
    volume_24h = models.DecimalField(max_digits=30, decimal_places=8)

    daily_signal = models.CharField(max_length=10)
    weekly_signal = models.CharField(max_length=10)
    monthly_signal = models.CharField(max_length=10)

    updated_at = models.DateTimeField(auto_now=True)

