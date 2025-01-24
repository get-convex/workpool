import { cronJobs } from "convex/server";
// import { internal } from "./_generated/api";

const crons = cronJobs();

// /* Useful for testing while developing.*/
// crons.interval(
//   "start background work",
//   { minutes: 1 }, // every minute
//   internal.example.startBackgroundWork
// );

// crons.interval(
//   "start foreground work",
//   { seconds: 20 }, // every 20 seconds
//   internal.example.startForegroundWork
// );

export default crons;
