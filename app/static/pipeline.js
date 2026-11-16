// 阶段卡与重试：失败阶段打开发起重跑，成功后回到总览。
const state = { busy: false };

function notice(message, isError = false) {
  const box = document.getElementById("notice");
  if (!box) return;
  box.textContent = message;
  box.classList.toggle("is-error", Boolean(isError));
  box.hidden = false;
}

async function retry(batchId) {
  if (state.busy) return;
  state.busy = true;
  try {
    const response = await fetch("/api/batches/" + encodeURIComponent(batchId) + "/retry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error((payload.error || response.status) + " " + (payload.message || ""));
    }
    notice("批次 " + payload.batch_id + " · " + payload.status);
    window.setTimeout(() => window.location.reload(), 500);
  } catch (error) {
    notice(String(error.message || error), true);
  } finally {
    state.busy = false;
  }
}

document.querySelectorAll(".retry-button").forEach((button) => {
  button.addEventListener("click", () => {
    const batchId = button.dataset.batch;
    if (batchId) retry(batchId);
  });
});

async function postBatch(batchId, action) {
  if (state.busy) return null;
  state.busy = true;
  try {
    const response = await fetch(
      "/api/batches/" + encodeURIComponent(batchId) + "/" + action,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error((payload.error || response.status) + " " + (payload.message || ""));
    }
    return payload;
  } catch (error) {
    notice(String(error.message || error), true);
    return null;
  } finally {
    state.busy = false;
  }
}

const redactButton = document.getElementById("redact-button");
if (redactButton && !redactButton.disabled) {
  redactButton.addEventListener("click", async () => {
    const confirmed = window.confirm(
      "清除本批次的原文？清除后原始文本不可恢复，但规范化读数与四账仍可按原批次复算。");
    if (!confirmed) return;
    const batchId = redactButton.dataset.batch;
    const payload = await postBatch(batchId, "redact");
    if (!payload) return;
    notice("批次 " + payload.batch_id + " · 原文" +
      (payload.state === "cleared" ? "已清除" : "清除中"));
    window.setTimeout(() => window.location.reload(), 500);
  });
}

const recomputeButton = document.getElementById("recompute-button");
if (recomputeButton) {
  recomputeButton.addEventListener("click", async () => {
    const batchId = recomputeButton.dataset.batch;
    const payload = await postBatch(batchId, "recompute");
    if (!payload) return;
    const balanced = payload.reconcile && payload.reconcile.all_balanced;
    notice("批次 " + payload.batch_id + " · 按原批次复算 · " +
      (balanced ? "四账平衡" : "四账不平"));
    window.setTimeout(() => window.location.reload(), 500);
  });
}

// 阶段卡：失败阶段高亮，其余按完成态显示序号
document.querySelectorAll(".card").forEach((card) => {
  const badge = card.querySelector(".badge");
  if (badge && badge.textContent.trim() === "失败") {
    card.classList.add("card-failed");
  }
});
