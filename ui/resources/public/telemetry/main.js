var admin = new Admin({
  baseUrl: telemetryBaseUrl,
  runPath: "inspect"
});

admin.clickQuery = function(d) {
  $("#name").val(d.opts.name);
  $("#query").val(d.opts.query);
  $("#target").val(d.opts.target);
  if (d.opts.target) showAdvanced();
};

admin.testQuery = function(opts) {
  startQuery(opts.query);
};

admin.addTree("#query-tree");

function selectTree(type) {
  $("#type").val(type);
  window.location.hash = "#" + type;
  admin.selectTree(type);
};

function hideAdvanced() {
  $("#advanced").attr("data-visible", "");
  $("#advanced").html("advanced &raquo;");
  $("#advanced-container").hide();
};

function showAdvanced() {
  $("#advanced").attr("data-visible", true);
  $("#advanced").html("advanced &laquo;");
  $("#advanced-container").css({display:"inline"});
};

function toggleAdvanced() {
  if ($("#advanced").attr("data-visible")) {
    hideAdvanced();
  } else {
    showAdvanced();
  }
  return false;
};

var testXhr;
function startQuery(query) {
  if (query) {
    var count = 0;
    $("#test").html("Stop")
    $("#inspect-query").html(query)
    $("#inspect-progress").removeAttr("value").css({opacity: 1});
    $("#inspect-results").text("");
    testXhr = admin.runQuery({query: query}, function(d) {
      count = (count + 1) % 100;
      $("#inspect-progress").attr({value: count});
      $("#inspect-results").text(d).scrollTop(999999999);
    }, stopQuery);
  }
};

function stopQuery() {
  testXhr.abort();
  testXhr = null;
  $("#test").html("Test");
  $("#inspect-progress").css({opacity: 0.3})
  setTimeout(function() {
    $("#inspect-progress").attr({value: 0});
  }, 100);
};

function toggleQuery() {
  if (testXhr) {
    stopQuery();
  } else {
    startQuery($("#query").val() || $("#inspect-query").text());
  }
};

$(window).on("hashchange", function() {
  selectTree(window.location.hash.substr(1));
});

$(document).ready(function() {
  $("#type").change(function(e) {
    selectTree($(this).val());
  });

  admin.addOpts("type", "#type");
  selectTree(window.location.hash.substr(1) || "phonograph");

  $("#add").click(function(e) {
    var $button = $(this);
    $button.attr("disabled", true);
    admin.addQuery({
      name:   $("#name").val(),
      target: $("#target").val(),
      type:   $("#type").val(),
      query:  $("#query").val(),
      "replay-since": $("#replay-since").val()
    }, function(d) {
      $button.attr("disabled", false);
    });
  });

  $("#test").click(toggleQuery);
  $("#advanced").click(toggleAdvanced);

  $("#name").focus(function(e) { $(this).attr("placeholder", "query:name") });
  $("#name").blur(function(e)  { $(this).attr("placeholder", "name")       });

  $("#target").focus(function(e) { $(this).attr("placeholder", $("#name").val() || "target:name") });
  $("#target").blur(function(e)  { $(this).attr("placeholder", "target")                          });

  $("#replay-since").focus(function(e) { $(this).attr("placeholder", "-7d")          });
  $("#replay-since").blur(function(e)  { $(this).attr("placeholder", "replay since") });
});
