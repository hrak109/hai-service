<?php
/**
 * Plugin Name: Oakhill Pines AI
 * Description: LLM powered chat plugin for Oakhill Pines website.
 * Version: 1.0
 * Author: Hee Bae
 */

function ai_chat_enqueue_scripts() {
    wp_enqueue_style('ai-chat-style', plugin_dir_url(__FILE__) . 'chat.css');
    wp_enqueue_script('ai-chat-script', plugin_dir_url(__FILE__) . 'chat.js', array('jquery'), null, true);

    // Pass the API URL to JavaScript
    wp_localize_script('ai-chat-script', 'aiChat', array(
        'apiUrl' => 'api.oakhillpines.com'
    ));
}
add_action('wp_enqueue_scripts', 'ai_chat_enqueue_scripts');

function ai_chat_shortcode() {
    ob_start(); ?>
    <div id="ai-chat-container">
        <div id="ai-chat-messages"></div>
        <div id="ai-chat-input-area">
            <input type="text" id="ai-chat-input" placeholder="Ask a question..." />
            <button id="ai-chat-send">Send</button>
        </div>
    </div>
    <?php
    return ob_get_clean();
}
add_shortcode('ai_chat', 'ai_chat_shortcode');
