// +build windows

package nsqd

// On Windows, file names cannot contain colons.
// 将两者名字拼接起来，windows 使用 ; 其余使用 :
func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>;<channel>
	backendName := topicName + ";" + channelName
	return backendName
}
