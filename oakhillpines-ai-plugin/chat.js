document.addEventListener("DOMContentLoaded", () => {
  const apiUrl = aiChat.apiUrl;
  const messagesDiv = document.getElementById("ai-chat-messages");
  const input = document.getElementById("ai-chat-input");
  const sendBtn = document.getElementById("ai-chat-send");

  function appendMessage(sender, text) {
    const msg = document.createElement("div");
    msg.className = `chat-message ${sender}`;
    msg.textContent = text;
    messagesDiv.appendChild(msg);
    messagesDiv.scrollTop = messagesDiv.scrollHeight;
  }

  if (!sessionStorage.getItem("greeted")) {
        appendMessage("ai", "Hi. I am Oakhill Pines AI! How can I help you today?");
        sessionStorage.setItem("greeted", "true");
    }

  async function sendQuestion() {
    const question = input.value.trim();
    if (!question) return;

    appendMessage("user", question);
    input.value = "";
    appendMessage("ai", "⏳ Thinking...");

    try {
      const res = await fetch(`${apiUrl}/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text: question }),
      });
      const data = await res.json();
      pollAnswer(data.question_id);
    } catch (err) {
      appendMessage("ai", "❌ AI service currently not available.");
    }
  }

  async function pollAnswer(qid) {
    let answered = false;
    const interval = setInterval(async () => {
      try {
        const res = await fetch(`${apiUrl}/get_answer/${qid}`);
        if (!res.ok) return;

        const data = await res.json();
        if (data.status === "answered") {
          const last = messagesDiv.lastElementChild;
          if (last && last.textContent === "⏳ Thinking...") last.remove();
          appendMessage("ai", data.answer);
          clearInterval(interval);
          answered = true;
        }
      } catch (err) {
        console.error("Polling error:", err);
      }
    }, 5000);

    // Timeout after 2 mins
    setTimeout(() => {
      if (!answered) {
        clearInterval(interval);
        appendMessage("ai", "⌛ Response timed out.");
      }
    }, 120000);
  }

  sendBtn.addEventListener("click", sendQuestion);
  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") sendQuestion();
  });
});
