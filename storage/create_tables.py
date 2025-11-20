from app import Base, mysql


#Needs to run first before anything 
Base.metadata.create_all(mysql)
print("Tables created")
