package ddldiff

import (
	"github.com/pagarme/teleport/action"
)

// Diff two arrays of Diffables
func Diff(preObjs []Diffable, postObjs []Diffable) []action.Action {
	actions := make([]action.Action, 0)

	// First check for new or updated objects (and their children)
	for _, post := range postObjs {
		var preObj Diffable

		for _, pre := range preObjs {
			if post.IsEqual(pre) {
				preObj = pre
				break
			}
		}

		actions = append(actions, post.Diff(preObj)...)

		if preObj != nil {
			actions = append(actions, Diff(preObj.Children(), post.Children())...)
		}
	}

	// Then, check for objects that are not present in post
	// (and therefore should be dropped)
	for _, pre := range preObjs {
		var postObj Diffable

		for _, post := range postObjs {
			if pre.IsEqual(post) {
				postObj = post
				break
			}
		}

		if postObj == nil {
			actions = append(actions, pre.Drop()...)
		}
	}

	return actions
}
