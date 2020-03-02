from tkinter import *
import folium
from ttkthemes import ThemedTk
import webbrowser

def system(location,radius,timestamp):
    result = {"Rotunda, Parnell Square West":(53.3522443611383,-6.26372321891887),"Rotunda, Granby Place":(53.3523085514325,-6.26381074216825)}
    return result

def getmap(coordinates):

    webbrowser.open("plot_data.html")

def submit(button):
    coordinates =(lat.get(),long.get())
    coordinates = (53.352350,-6.264902)
    radius = r.get()
    timestamp = ts.get()
    stops = system(coordinates,radius,timestamp)
    m = folium.Map(location=coordinates,zoom_start=30)
    folium.Marker(coordinates,icon=folium.Icon(color="orange",icon="taxi", prefix='fa'), popup='<b> Your Location </b>').add_to(m)
    folium.Circle(radius=radius,location=coordinates,color='crimson',fill=False).add_to(m)
    for stop_name in stops.keys():
        folium.Marker(stops[stop_name],icon=folium.Icon(color="blue",icon="bus", prefix='fa'), popup='<b>'+stop_name+'</b>').add_to(m)
    m.save("plot_data.html")
    button.grid(row=4)

window = Tk()


window.geometry("1000x1000")

window.title("Relaxi-Taxi")

coordinates = [45.5236, -122.6750]


b = Button(window, text="Map",command =lambda: getmap(coordinates))

L1 = Label(window, text="Latitude:").grid(row=0)
L2 = Label(window, text="Longitude:").grid(row=0,column=2)
L3 = Label(window, text="Radius:").grid(row=1)
L4 = Label(window, text="Timestamp:").grid(row=2)

lat = Entry(window, bd =5)
lat.grid(row=0,column=1)
long = Entry(window, bd =5)
long.grid(row=0,column=3)
r = Entry(window, bd =5)
r.grid(row=1,column=1)
ts = Entry(window, bd =5)
ts.grid(row=2,column=1)


Button(window,text="Submit",command = lambda: submit(b)).grid(row=3,column=1)
#ttk.Button(window, text="Quit", command=window.destroy).pack()




window.mainloop()