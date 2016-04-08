package ddldiff

func Diff(preObjs []Diffable, postObjs []Diffable) []Action {
	actions := make([]Action, 0)

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
