from app import Base, mysql

Base.metadata.drop_all(mysql)
print("Tables dropped")
