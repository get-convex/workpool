import "./App.css";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";

function App() {
  const list = useQuery(api.example.list);
  const addNow = useMutation(api.example.addNow);
  const addLater = useMutation(api.example.addLater);

  return (
    <>
      <h1>Convex Workpool Component Example</h1>
      <div className="card">
        <button onClick={() => addNow({})}>add some now yo</button>
        <button onClick={() => addLater({})}>add some whenever dogg</button>
        <div>
          <h3>This is the stuff:</h3>
          <div style={{ listStyle: "none" }}>
            {list?.map((item) => <div key={item._id}>{item.data}</div>)}
          </div>
        </div>
      </div>
    </>
  );
}

export default App;
