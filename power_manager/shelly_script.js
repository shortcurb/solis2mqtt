// Have your shelly device run this as a script. Publishes power twice a second
// Publish combined split-phase house power from Shelly Pro 3EM
// Positive = importing from grid
// Negative = exporting to grid

let TOPIC = "shellypro3em-6825ddd2b968/power/total_power";

let INTERVAL_MS = 500;

Timer.set(INTERVAL_MS, true, function () {
  Shelly.call("Shelly.GetStatus", {}, function (status, error_code, error_message) {
    if (error_code !== 0) {
      print("Shelly.GetStatus error:", error_message);
      return;
    }

    let leg_a = status["em1:0"];
    let leg_b = status["em1:2"];

    if (leg_a === undefined || leg_b === undefined) {
      print("Missing em1 status objects");
      return;
    }

    let total_power = leg_a.act_power + leg_b.act_power;

    MQTT.publish(
      TOPIC,
      JSON.stringify({
        total_power: total_power * 4,
        leg_a_power: leg_a.act_power * 4,
        leg_b_power: leg_b.act_power * 4,
        ts: Date.now()
      }),
      1,
      false
    );

    print("Published total_power:", total_power);
  });
});
