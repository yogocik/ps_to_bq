import datetime
from faker import Faker

fake = Faker()

def current_epoch_ms() -> int:
    return round(datetime.datetime.now().timestamp() * 1000)

def current_epoch_delta_ms(hour:int=0, minute:int=0, second:int=0) -> int:
    added = (datetime.datetime.now() + 
            datetime.timedelta(hours=hour, 
                               minutes=minute, seconds=second)).timestamp() * 1000
    return round(added)

def fake_name():
    return fake.name()

def main():
    print(current_epoch_ms())
    print(current_epoch_delta_ms(hour=1))
    print(fake_name())
